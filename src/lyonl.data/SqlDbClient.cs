﻿// Copyright (c) 1999-2018 LyonL Interactive Inc. 
// All rights reserved.
//  
// Permission is hereby granted, free of charge, to
// any person obtaining a copy of this software and
// associated documentation files (the "Software"),
// to deal in the Software without restriction,
// including without limitation the rights to use,
// copy, modify, merge, publish, distribute,
// sublicense, and/or sell copies of the Software,
// and to permit persons to whom the Software is
// furnished to do so, subject to the following
// conditions: The above copyright notice and this
// permission notice shall be included in all copies
// or substantial portions of the Software. 
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY
// OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
// LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
// OR OTHER LIABILITY, WHETHER IN AN ACTION OF
// CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF
// OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

using System;
using System.Data.Common;
using System.Data.SqlClient;

// ReSharper disable once CheckNamespace
namespace LyonL.Data
{
    public class SqlDbClient : DbClient<SqlException>
    {
        public SqlDbClient(Func<DbConnection> connectionFactory) : base(connectionFactory)
        {
        }

        protected override bool SqlRetry(SqlException ex)
        {
            return ex.Number != -2 || ex.Number != 11 || ex.Number != 1205 || ex.Number != 11001;
        }
    }
}