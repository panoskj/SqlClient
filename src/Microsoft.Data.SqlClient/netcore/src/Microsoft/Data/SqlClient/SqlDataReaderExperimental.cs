using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Data.SqlClient
{
    partial class SqlDataReader
    {
        /// <summary>
        /// This flag is for enabling experimental async performance fixes.
        /// Makes it easier to run benchmarks.
        /// </summary>
        public static bool Experimental_TdsParserObject_TryReadSni { get; set; }

        /// <summary>
        /// This flag is for enabling experimental async performance fixes.
        /// Makes it easier to run benchmarks.
        /// Requires setting <see cref="Experimental_TdsParserObject_TryReadSni"/> to true.
        /// </summary>
        public static bool Experimental_TdsParserStateObject_EnsureEnoughDataForPlp { get; set; }

        /// <summary>
        /// Header size per packet, overestimating this value results in trying to read more packets than possible, hanging the client.
        /// This flag is meant for benchmarking and should be set to 0 to ensure the code is 100% correct. A value of 12 works for my tests.
        /// </summary>
        public const int Experimental_PlpHeaderSize = 12;

        /// <summary>
        /// Counts how many times <see cref="TdsParser.TryReadSqlStringValueExperimental"/> continued after calling 
        /// <see cref="TdsParserStateObject.EnsureEnoughDataForPlp"/>.
        /// Under normal circumstances and if <see cref="Experimental_PlpHeaderSize"/> is 12, 
        /// <see cref="Experimental_TryReadCounter"/> will be incremented by one,
        /// for each string column that is read asynchronously (tested for up to 20M chars - 40MB).
        /// If you set <see cref="Experimental_PlpHeaderSize"/> to 0, 
        /// this counter can be incremented more than once per string column,
        /// depending on column size (e.g. for columns larger than 5MB).
        /// </summary>
        public static int Experimental_TryReadCounter { get; set; }
    }
}
