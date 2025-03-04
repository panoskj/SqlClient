﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading;

namespace Microsoft.Data.SqlClient
{
    internal class NoneAttestationEnclaveProvider : EnclaveProviderBase
    {
        private static readonly int EnclaveSessionHandleSize = 8;
        private const int DiffieHellmanKeySize = 384;
        private const int NoneAttestationProtocolId = (int)SqlConnectionAttestationProtocol.None;


        // When overridden in a derived class, looks up an existing enclave session information in the enclave session cache.
        // If the enclave provider doesn't implement enclave session caching, this method is expected to return null in the sqlEnclaveSession parameter.
        internal override void GetEnclaveSession(EnclaveSessionParameters enclaveSessionParameters, bool generateCustomData, out SqlEnclaveSession sqlEnclaveSession, out long counter, out byte[] customData, out int customDataLength)
        {
            GetEnclaveSessionHelper(enclaveSessionParameters, false, out sqlEnclaveSession, out counter, out customData, out customDataLength);
        }

        // Gets the information that SqlClient subsequently uses to initiate the process of attesting the enclave and to establish a secure session with the enclave.
        internal override SqlEnclaveAttestationParameters GetAttestationParameters(string attestationUrl, byte[] customData, int customDataLength)
        {
            ECDiffieHellman clientDHKey = KeyConverter.CreateECDiffieHellman(DiffieHellmanKeySize);
            return new SqlEnclaveAttestationParameters(NoneAttestationProtocolId, Array.Empty<byte>(), clientDHKey);
        }

        // When overridden in a derived class, performs enclave attestation, generates a symmetric key for the session, creates an enclave session and stores the session information in the cache.
        internal override void CreateEnclaveSession(byte[] attestationInfo, ECDiffieHellman clientDHKey, EnclaveSessionParameters enclaveSessionParameters, byte[] customData, int customDataLength, out SqlEnclaveSession sqlEnclaveSession, out long counter)
        {
            // for None attestation: enclave does not send public key, and sends an empty attestation info
            // The only non-trivial content it sends is the session setup info (DH pubkey of enclave)

            sqlEnclaveSession = null;
            counter = 0;
            try
            {
                ThreadRetryCache.Remove(Thread.CurrentThread.ManagedThreadId.ToString());
                sqlEnclaveSession = GetEnclaveSessionFromCache(enclaveSessionParameters, out counter);

                if (sqlEnclaveSession == null)
                {
                    // Read AttestationInfo
                    int attestationInfoOffset = 0;
                    uint sizeOfTrustedModuleAttestationInfoBuffer = BitConverter.ToUInt32(attestationInfo, attestationInfoOffset);
                    attestationInfoOffset += sizeof(UInt32);
                    int sizeOfTrustedModuleAttestationInfoBufferInt = checked((int)sizeOfTrustedModuleAttestationInfoBuffer);
                    Debug.Assert(sizeOfTrustedModuleAttestationInfoBuffer == 0);

                    // read secure session info
                    uint sizeOfSecureSessionInfoResponse = BitConverter.ToUInt32(attestationInfo, attestationInfoOffset);
                    attestationInfoOffset += sizeof(UInt32);

                    byte[] enclaveSessionHandle = new byte[EnclaveSessionHandleSize];
                    Buffer.BlockCopy(attestationInfo, attestationInfoOffset, enclaveSessionHandle, 0, EnclaveSessionHandleSize);
                    attestationInfoOffset += EnclaveSessionHandleSize;

                    uint sizeOfTrustedModuleDHPublicKeyBuffer = BitConverter.ToUInt32(attestationInfo, attestationInfoOffset);
                    attestationInfoOffset += sizeof(UInt32);
                    uint sizeOfTrustedModuleDHPublicKeySignatureBuffer = BitConverter.ToUInt32(attestationInfo, attestationInfoOffset);
                    attestationInfoOffset += sizeof(UInt32);
                    int sizeOfTrustedModuleDHPublicKeyBufferInt = checked((int)sizeOfTrustedModuleDHPublicKeyBuffer);

                    byte[] trustedModuleDHPublicKey = new byte[sizeOfTrustedModuleDHPublicKeyBuffer];
                    Buffer.BlockCopy(attestationInfo, attestationInfoOffset, trustedModuleDHPublicKey, 0,
                        sizeOfTrustedModuleDHPublicKeyBufferInt);
                    attestationInfoOffset += sizeOfTrustedModuleDHPublicKeyBufferInt;

                    byte[] trustedModuleDHPublicKeySignature = new byte[sizeOfTrustedModuleDHPublicKeySignatureBuffer];
                    Buffer.BlockCopy(attestationInfo, attestationInfoOffset, trustedModuleDHPublicKeySignature, 0,
                        checked((int)sizeOfTrustedModuleDHPublicKeySignatureBuffer));

                    byte[] sharedSecret;
                    using ECDiffieHellman ecdh = KeyConverter.CreateECDiffieHellmanFromPublicKeyBlob(trustedModuleDHPublicKey);
                    sharedSecret = KeyConverter.DeriveKey(clientDHKey, ecdh.PublicKey);
                    long sessionId = BitConverter.ToInt64(enclaveSessionHandle, 0);
                    sqlEnclaveSession = AddEnclaveSessionToCache(enclaveSessionParameters, sharedSecret, sessionId, out counter);

                    if (sqlEnclaveSession is null)
                    {
                        throw SQL.AttestationFailed(Strings.FailToCreateEnclaveSession);
                    }
                }
            }
            finally
            {
                UpdateEnclaveSessionLockStatus(sqlEnclaveSession);
            }
        }

        /// <summary>
        /// When overridden in a derived class, looks up and evicts an enclave session from the enclave session cache, if the provider implements session caching.
        /// </summary>
        /// <param name="enclaveSessionParameters">The set of parameters required for enclave session.</param>
        /// <param name="enclaveSessionToInvalidate">The session to be invalidated.</param>
        internal override void InvalidateEnclaveSession(EnclaveSessionParameters enclaveSessionParameters, SqlEnclaveSession enclaveSessionToInvalidate)
        {
            InvalidateEnclaveSessionHelper(enclaveSessionParameters, enclaveSessionToInvalidate);
        }
    }
}
