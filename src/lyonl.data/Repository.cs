// Copyright (c) 1999-2018 LyonL Interactive Inc. 
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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using LyonL.Data.Specifications;

// ReSharper disable once CheckNamespace
namespace LyonL.Data
{
    public sealed class Repository<T> : IRepository<T>
    {
        internal readonly Func<IDbClient> DbClientFactory;

        public Repository(Func<IDbClient> dbClientFactory)
        {
            DbClientFactory = dbClientFactory ?? throw new ArgumentNullException(nameof(dbClientFactory));
        }

        public void ExecuteDbAction(Action<IDbClient> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));

            using (var client = DbClientFactory())
            {
                action(client);
            }
        }

        public T ExecuteDbAction(Func<IDbClient, T> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));

            using (var client = DbClientFactory())
            {
                return action(client);
            }
        }

        public Task ExecuteDbActionAsync(Func<IDbClient, Task> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));

            using (var client = DbClientFactory())
            {
                return action(client);
            }
        }

        public Task<IEnumerable<T>> ExecuteDbActionAsync(Func<IDbClient, Task<IEnumerable<T>>> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));

            using (var client = DbClientFactory())
            {
                return action(client);
            }
        }

        public Task ExecuteDbActionAsync(INonQuerySpecification specification, CancellationToken cancellationToken)
        {
            return ExecuteDbActionAsync(specification.ExecuteFunc(cancellationToken));
        }

        public Task<IEnumerable<T>> ExecuteDbActionAsync(IQuerySpecification<T> specification,
            CancellationToken cancellationToken)
        {
            return ExecuteDbActionAsync(specification.ExecuteFunc(cancellationToken));
        }
    }
}