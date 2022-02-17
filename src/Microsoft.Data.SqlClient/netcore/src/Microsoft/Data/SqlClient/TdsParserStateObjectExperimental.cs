using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Common;

namespace Microsoft.Data.SqlClient
{
    partial class TdsParserStateObject
    {
        /// <summary>
        /// Same as <see cref="ReadSni"/>, but returns true instead of false, if <see cref="ReadAsync"/> succeeds synchronously.
        /// </summary>
        /// <param name="completion"></param>
        /// <returns></returns>
        internal bool TryReadSni(TaskCompletionSource<object> completion)
        {
            Debug.Assert(_networkPacketTaskSource == null || ((_asyncReadWithoutSnapshot) && (_networkPacketTaskSource.Task.IsCompleted)), "Pending async call or failed to replay snapshot when calling ReadSni");
            _networkPacketTaskSource = completion;

            // Ensure that setting the completion source is completed before checking the state
            Interlocked.MemoryBarrier();

            // We must check after assigning _networkPacketTaskSource to avoid races with
            // SqlCommand.OnConnectionClosed
            if (_parser.State == TdsParserState.Broken || _parser.State == TdsParserState.Closed)
            {
                throw ADP.ClosedConnectionError();
            }

#if DEBUG
            if (_forcePendingReadsToWaitForUser)
            {
                _realNetworkPacketTaskSource = new TaskCompletionSource<object>();
            }
#endif


            PacketHandle readPacket = default;

            uint error = 0;

            bool result = false;

            try
            {
                Debug.Assert(completion != null, "Async on but null asyncResult passed");

                // if the state is currently stopped then change it to running and allocate a new identity value from 
                // the identity source. The identity value is used to correlate timer callback events to the currently
                // running timeout and prevents a late timer callback affecting a result it does not relate to
                int previousTimeoutState = Interlocked.CompareExchange(ref _timeoutState, TimeoutState.Running, TimeoutState.Stopped);
                Debug.Assert(previousTimeoutState == TimeoutState.Stopped, "previous timeout state was not Stopped");
                if (previousTimeoutState == TimeoutState.Stopped)
                {
                    Debug.Assert(_timeoutIdentityValue == 0, "timer was previously stopped without resetting the _identityValue");
                    _timeoutIdentityValue = Interlocked.Increment(ref _timeoutIdentitySource);
                }

                _networkPacketTimeout?.Dispose();

                _networkPacketTimeout = ADP.UnsafeCreateTimer(
                    _onTimeoutAsync,
                    new TimeoutState(_timeoutIdentityValue),
                    Timeout.Infinite,
                    Timeout.Infinite
                );


                // -1 == Infinite
                //  0 == Already timed out (NOTE: To simulate the same behavior as sync we will only timeout on 0 if we receive an IO Pending from SNI)
                // >0 == Actual timeout remaining
                int msecsRemaining = GetTimeoutRemaining();
                if (msecsRemaining > 0)
                {
                    ChangeNetworkPacketTimeout(msecsRemaining, Timeout.Infinite);
                }

                Interlocked.Increment(ref _readingCount);

                SessionHandle handle = SessionHandle;
                if (!handle.IsNull)
                {
                    IncrementPendingCallbacks();

                    readPacket = ReadAsync(handle, out error);

                    if (!(TdsEnums.SNI_SUCCESS == error || TdsEnums.SNI_SUCCESS_IO_PENDING == error))
                    {
                        DecrementPendingCallbacks(false); // Failure - we won't receive callback!
                    }
                }

                Interlocked.Decrement(ref _readingCount);

                if (handle.IsNull)
                {
                    throw ADP.ClosedConnectionError();
                }

                if (TdsEnums.SNI_SUCCESS == error)
                { // Success - process results!
                    result = true;
                    Debug.Assert(IsValidPacket(readPacket), "ReadNetworkPacket should not have been null on this async operation!");
                    // Evaluate this condition for MANAGED_SNI. This may not be needed because the network call is happening Async and only the callback can receive a success.
                    ReadAsyncCallback(IntPtr.Zero, readPacket, 0);
                    // Call ReadAsyncCallback to ensure anyone waiting for _networkPacketTaskSource can continue (may be unnecessary).
                    _networkPacketTaskSource = null;

                    // Only release packet for Managed SNI as for Native SNI packet is released in finally block.
                    if (TdsParserStateObjectFactory.UseManagedSNI && !IsPacketEmpty(readPacket))
                    {
                        ReleasePacket(readPacket);
                    }
                }
                else if (TdsEnums.SNI_SUCCESS_IO_PENDING != error)
                { // FAILURE!
                    Debug.Assert(IsPacketEmpty(readPacket), "unexpected readPacket without corresponding SNIPacketRelease");

                    ReadSniError(this, error);
#if DEBUG
                    if ((_forcePendingReadsToWaitForUser) && (_realNetworkPacketTaskSource != null))
                    {
                        _realNetworkPacketTaskSource.TrySetResult(null);
                    }
                    else
#endif
                    {
                        _networkPacketTaskSource.TrySetResult(null);
                    }
                    // Disable timeout timer on error
                    SetTimeoutStateStopped();
                    ChangeNetworkPacketTimeout(Timeout.Infinite, Timeout.Infinite);
                }
                else if (msecsRemaining == 0)
                {
                    // Got IO Pending, but we have no time left to wait
                    // disable the timer and set the error state by calling OnTimeoutSync
                    ChangeNetworkPacketTimeout(Timeout.Infinite, Timeout.Infinite);
                    OnTimeoutSync();
                }
                // DO NOT HANDLE PENDING READ HERE - which is TdsEnums.SNI_SUCCESS_IO_PENDING state.
                // That is handled by user who initiated async read, or by ReadNetworkPacket which is sync over async.
            }
            finally
            {
                if (!TdsParserStateObjectFactory.UseManagedSNI)
                {
                    if (!IsPacketEmpty(readPacket))
                    {
                        // Be sure to release packet, otherwise it will be leaked by native.
                        ReleasePacket(readPacket);
                    }
                }
                AssertValidState();
            }

            return result;
        }


        /// <summary>
        /// Returns true if there are enough packets in the snapshot to read the current PLP data. Otherwise it requests more packets from the driver.
        /// If there are not enough packets in the snapshot:
        /// 1. Try to read as many packets as possible without blocking, pushing them in the snapshot.
        /// 2. Read one more packet, starting an asynchronous operation.
        /// 3. Return false to the caller, resulting in the read operation being continued when the asynchronous operation completes (e.g. when another packet arrives).
        /// Notes:
        /// 1. This implementation isn't 100% correct, it should restore the state of the snapshots in case it manages to read enough packets without blocking, 
        ///     returning true to the caller. Currently it returns false and the operation is retried, in order to replay reset and replay the snapshot properly.
        /// </summary>
        /// <returns></returns>
        internal bool EnsureEnoughDataForPlp()
        {
            if (_syncOverAsync)
            {
                // There is always enough data available for sync mode (because we block until it is).
                return true;
            }

            if (_longlen == TdsEnums.SQL_PLP_NULL 
                || _longlen == TdsEnums.SQL_PLP_UNKNOWNLEN 
                || _snapshot == null)
            {
                // We don't know how to handle these cases, let the caller firgure it out.
                return true;
            }

            Debug.Assert(_inBytesUsed >= SqlDataReader.Experimental_PlpHeaderSize, "The packet header and PLP prefix should have been read.");

            ulong inBytesUsed = (ulong)_inBytesUsed - SqlDataReader.Experimental_PlpHeaderSize;

            if (_longlen <= _snapshot._inBytesReadTotal - inBytesUsed)
            {
                // We know there is enough data to read the PLP data, let the caller continue.
                return true;
            }

            // Replay all packets, because we want to read more data now.
            _snapshot.ReplayAll();

            // Try reading packets without blocking.
            while (TryReadNetworkPacket())
            {
                if (_longlen <= _snapshot._inBytesReadTotal - inBytesUsed)
                {
                    // Return false to force the caller to retry the operation (so that the snapshot gets reset and replayed).
                    // A better solution would be to reset the snapshot here (we need to keep a "checkpoint" before calling ReplayAll to do this).
                    // If the snapshot was reset here, we could return true to the caller.


                    // TryReadSni sets _networkPacketTaskSource to null if it returns true
                    // We have to create a new completed _networkPacketTaskSource in order for the caller
                    // to retry after returning false from here.

                    _networkPacketTaskSource = new TaskCompletionSource<object>();
                    _networkPacketTaskSource.TrySetResult(null);

                    return false;
                }
            }

            // There is not enough data available right now, the caller has to wait for a callback from the driver before retrying.
            return false;
        }


        partial class StateSnapshot
        {
            internal ulong _inBytesReadTotal;

            /// <summary>
            /// Same as calling Replay() repeatedly until it returns false, but a lot faster.
            /// Benchmarks show that traversing the liked-list implementation was a major performance hit (when having 1000+ packets buffered).
            /// This indicates memory may be getting highly fragmented.
            /// You can try replacing this method's body with Replay in a loop to see the difference.
            /// </summary>
            public void ReplayAll()
            {
                if (_snapshotInBuffCurrent < _snapshotInBuffCount)
                {
                    _stateObj._inBuff = _snapshotInBuffList.Buffer;
                    _stateObj._inBytesUsed = 0;
                    _stateObj._inBytesRead = _snapshotInBuffList.Read;
                    _snapshotInBuffCurrent = _snapshotInBuffCount;
                }
            }
        }
    }
}
