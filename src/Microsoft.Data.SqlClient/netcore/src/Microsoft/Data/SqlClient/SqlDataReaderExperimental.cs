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
        public static bool Experimental { get; set; }
    }
}
