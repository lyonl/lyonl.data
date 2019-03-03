using System;
using System.Threading;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace LyonL.Data.Specifications
{
    public interface INonQuerySpecification
    {
        Func<IDbClient, Task> ExecuteFunc(CancellationToken cancellationToken);
    }
}