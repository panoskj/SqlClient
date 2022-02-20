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


        internal bool TryReadPlpColumn(out byte[] buffer, out int bufferLength)
        {
            buffer = Array.Empty<byte>();
            bufferLength = 0;

            if (_snapshot._plpExtra == null)
            {
                _snapshot._plpExtra = new StateSnapshot.PlpExtra();
            }
            else
            {
                _snapshot.RestorePlpExtra();

                if (!TryPrepareBuffer())
                {
                    return false;
                }                
            }

            StateSnapshot.PlpExtra extra = _snapshot._plpExtra;

            bool finished = TryReadPlpBytes_Extra(ref extra._plpBuff, extra._plpBuffBytesUsed, int.MaxValue, out int totalBytesRead);

            extra._plpBuffBytesUsed += totalBytesRead;

            extra.Check();


            _snapshot.SavePlpExtra();

            buffer = extra._plpBuff;
            bufferLength = extra._plpBuffBytesUsed;

            if (finished)
            {
                extra.Reset();
                _snapshot._plpExtra = null;
            }

            return finished;
        }

        // Reads the requested number of bytes from a plp data stream, or the entire data if
        // requested length is -1 or larger than the actual length of data. First call to this method
        //  should be preceeded by a call to ReadPlpLength or ReadDataLength.
        // Returns the actual bytes read.
        // NOTE: This method must be retriable WITHOUT replaying a snapshot
        // Every time you call this method increment the offset and decrease len by the value of totalBytesRead
        internal bool TryReadPlpBytes_Extra(ref byte[] buff, int offset, int len, out int totalBytesRead)
        {
            int bytesRead;
            int bytesLeft;
            ulong ignored;

            if (_longlen == 0)
            {
                Debug.Assert(_longlenleft == 0);
                if (buff == null)
                {
                    buff = Array.Empty<byte>();
                }

                AssertValidState();
                totalBytesRead = 0;
                return true;       // No data
            }

            Debug.Assert(_longlen != TdsEnums.SQL_PLP_NULL, "Out of sync plp read request");
            // Debug.Assert((buff == null && offset == 0) || (buff.Length >= offset + len), "Invalid length sent to ReadPlpBytes()!");

            bytesLeft = len;

            // If total length is known up front, allocate the whole buffer in one shot instead of realloc'ing and copying over each time
            if (buff == null && _longlen != TdsEnums.SQL_PLP_UNKNOWNLEN)
            {
                if (_snapshot != null)
                {
                    // if there is a snapshot and it contains a stored plp buffer take it
                    // and try to use it if it is the right length
                    buff = _snapshot._plpBuffer;
                    _snapshot._plpBuffer = null;
                }

                if ((ulong)(buff?.Length ?? 0) != _longlen)
                {
                    // if the buffer is null or the wrong length create one to use
                    buff = new byte[(int)Math.Min((int)_longlen, len)];
                }
            }

            if (_longlenleft == 0)
            {
                if (!TryReadPlpLength(false, out ignored))
                {
                    totalBytesRead = 0;
                    return false;
                }
                if (_longlenleft == 0)
                { // Data read complete
                    totalBytesRead = 0;
                    return true;
                }
            }

            if (buff == null)
            {
                buff = new byte[_longlenleft];
            }

            totalBytesRead = 0;

            while (bytesLeft > 0)
            {
                int bytesToRead = (int)Math.Min(_longlenleft, (ulong)bytesLeft);
                if (buff.Length < (offset + bytesToRead))
                {
                    // Grow the array
                    Array.Resize(ref buff, offset + bytesToRead);
                }

                bool result = TryReadByteArray(buff.AsSpan(offset), bytesToRead, out bytesRead);

                Debug.Assert(bytesRead <= bytesLeft, "Read more bytes than we needed");
                Debug.Assert((ulong)bytesRead <= _longlenleft, "Read more bytes than is available");

                bytesLeft -= bytesRead;
                offset += bytesRead;
                totalBytesRead += bytesRead;
                _longlenleft -= (ulong)bytesRead;
                if (!result)
                {
                    if (_snapshot != null)
                    {
                        // a partial read has happened so store the target buffer in the snapshot
                        // so it can be re-used when another packet arrives and we read again
                        _snapshot._plpBuffer = buff;
                    }
                    return false;
                }

                if (_longlenleft == 0)
                {
                    // Read the next chunk or cleanup state if hit the end
                    if (!TryReadPlpLength(false, out ignored))
                    {
                        if (_snapshot != null)
                        {
                            // a partial read has happened so store the target buffer in the snapshot
                            // so it can be re-used when another packet arrives and we read again
                            _snapshot._plpBuffer = buff;
                        }
                        return false;
                    }
                }

                AssertValidState();

                // Catch the point where we read the entire plp data stream and clean up state
                if (_longlenleft == 0)   // Data read complete
                    break;
            }
            return true;
        }


        partial class StateSnapshot
        {
            readonly IList<PacketData> _bufferedPackets = new List<PacketData>();
            internal ulong _inBytesReadTotal;
            internal PlpExtra _plpExtra;

            internal void RestorePlpExtra()
            {
                _snapshotInBuffCurrent = _plpExtra._snapshotInBuffCurrent;

                _stateObj._inBuff = _plpExtra._snapshotInBuff;
                _stateObj._inBytesRead = _plpExtra._snapshotInBytesRead;
                _stateObj._inBytesUsed = _plpExtra._snapshotInBytesUsed;
                _stateObj._inBytesPacket = _plpExtra._snapshotInBytesPacket;

                _stateObj._longlen = _plpExtra._longlen;
                _stateObj._longlenleft = _plpExtra._longlenleft;
            }

            internal void SavePlpExtra()
            {
                _plpExtra._snapshotInBuffCurrent = _snapshotInBuffCount;

                _plpExtra._snapshotInBuff = _stateObj._inBuff;
                _plpExtra._snapshotInBytesRead = _stateObj._inBytesRead;
                _plpExtra._snapshotInBytesUsed = _stateObj._inBytesUsed;
                _plpExtra._snapshotInBytesPacket = _stateObj._inBytesPacket;

                _plpExtra._longlen = _stateObj._longlen;
                _plpExtra._longlenleft = _stateObj._longlenleft;
            }

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


            internal bool ReplayUsingList()
            {
                if (_snapshotInBuffCurrent < _snapshotInBuffCount)
                {
                    int reverseIndex = _snapshotInBuffCount - _snapshotInBuffCurrent; // 1-based, reverse index
                    int forwardIndex = _bufferedPackets.Count - reverseIndex; // 0-based

                    PacketData currentPacket = _bufferedPackets[forwardIndex];
                 
                    _stateObj._inBuff = currentPacket.Buffer;
                    _stateObj._inBytesUsed = 0;
                    _stateObj._inBytesRead = currentPacket.Read;
                    _snapshotInBuffCurrent++;
                    return true;
                }

                return false;
            }

            void PushBufferExtra()
            {
                if (SqlDataReader.Experimental_TdsParserStateObject_ReplayUsingList)
                {
                    _bufferedPackets.Add(_snapshotInBuffList);
                }

                if (SqlDataReader.Experimental_TdsParserStateObject_EnsureEnoughDataForPlp)
                {
                    _inBytesReadTotal += (ulong)_snapshotInBuffList.Read;
                    if (_snapshotInBuffList.Read > 2 * SqlDataReader.Experimental_PlpHeaderSize)
                    {
                        _inBytesReadTotal -= SqlDataReader.Experimental_PlpHeaderSize;
                    }
                }
            }

            void ClearExtra()
            {
                if (SqlDataReader.Experimental_TdsParserStateObject_TryReadSqlStringValuePlp)
                {
                    _plpExtra = null;
                }

                if (SqlDataReader.Experimental_TdsParserStateObject_EnsureEnoughDataForPlp)
                {
                    _inBytesReadTotal = 0;
                }

                if (SqlDataReader.Experimental_TdsParserStateObject_ReplayUsingList)
                {
                    _bufferedPackets.Clear();
                }
            }


            internal class PlpExtra
            {
                internal byte[] _plpBuff;
                internal int _plpBuffBytesUsed;
                internal byte[] _snapshotInBuff;
                internal int _snapshotInBuffCurrent;
                internal int _snapshotInBytesRead;
                internal int _snapshotInBytesUsed;
                internal int _snapshotInBytesPacket;
                internal ulong _longlen;
                internal ulong _longlenleft;

                internal void Reset()
                {
                    _plpBuffBytesUsed = 0;
                    _snapshotInBuffCurrent = 0;
                    _snapshotInBytesUsed = 0;
                    _snapshotInBytesPacket = 0;
                    _longlen = 0;
                    _longlenleft = 0;
                }
            }
        }
    }
}
