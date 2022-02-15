using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Common;

namespace Microsoft.Data.SqlClient
{
    partial class TdsParserStateObject
    {
        static bool Experimental => SqlDataReader.Experimental;

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
    }
}
