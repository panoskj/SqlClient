using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Microsoft.Data.SqlClient
{
    partial class TdsParser
    {
        /// <summary>
        /// Same as <see cref="TryReadSqlStringValue"/>, but also checks if there is enough data in the snapshot before trying to read PLP data in async mode.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <param name="length"></param>
        /// <param name="encoding"></param>
        /// <param name="isPlp"></param>
        /// <param name="stateObj"></param>
        /// <returns></returns>
        private bool TryReadSqlStringValueExperimental(SqlBuffer value, byte type, int length, Encoding encoding, bool isPlp, TdsParserStateObject stateObj)
        {
            if (isPlp && !stateObj.EnsureEnoughDataForPlp())
            {
                return false;
            }

            SqlDataReader.Experimental_TryReadCounter += 1;

            switch (type)
            {
                case TdsEnums.SQLCHAR:
                case TdsEnums.SQLBIGCHAR:
                case TdsEnums.SQLVARCHAR:
                case TdsEnums.SQLBIGVARCHAR:
                case TdsEnums.SQLTEXT:
                    // If bigvarchar(max), we only read the first chunk here,
                    // expecting the caller to read the rest
                    if (encoding == null)
                    {
                        // if hitting 7.0 server, encoding will be null in metadata for columns or return values since
                        // 7.0 has no support for multiple code pages in data - single code page support only
                        encoding = _defaultEncoding;
                    }
                    string stringValue;
                    if (!stateObj.TryReadStringWithEncoding(length, encoding, isPlp, out stringValue))
                    {
                        return false;
                    }
                    value.SetToString(stringValue);
                    break;

                case TdsEnums.SQLNCHAR:
                case TdsEnums.SQLNVARCHAR:
                case TdsEnums.SQLNTEXT:
                    {
                        string s = null;

                        if (isPlp)
                        {
                            char[] cc = null;

                            if (!TryReadPlpUnicodeChars(ref cc, 0, length >> 1, stateObj, out length))
                            {
                                return false;
                            }
                            if (length > 0)
                            {
                                s = new string(cc, 0, length);
                            }
                            else
                            {
                                s = "";
                            }
                        }
                        else
                        {
                            if (!stateObj.TryReadString(length >> 1, out s))
                            {
                                return false;
                            }
                        }

                        value.SetToString(s);
                        break;
                    }

                default:
                    Debug.Fail("Unknown tds type for SqlString!" + type.ToString(CultureInfo.InvariantCulture));
                    break;
            }

            return true;
        }
    }
}
