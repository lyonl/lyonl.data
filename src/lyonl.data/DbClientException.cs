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
using System.Runtime.Serialization;
using System.Security.Permissions;

// ReSharper disable once CheckNamespace
namespace LyonL.Data
{
    [Serializable]
    public class DbClientException : Exception
    {
        public DbClientException()
        {
        }

        public DbClientException(string message)
            : base(message)
        {
        }

        public DbClientException(string message, Exception inner)
            : base(message, inner)
        {
        }

        protected DbClientException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
                throw new ArgumentNullException(nameof(info));
            base.GetObjectData(info, context);
        }
    }
}