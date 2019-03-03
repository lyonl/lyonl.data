using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace LyonL.Data.Specifications
{
    public interface IQuerySpecification<T>
    {
        Func<IDbClient, Task<IEnumerable<T>>> ExecuteFunc(CancellationToken cancellationToken);
    }
}