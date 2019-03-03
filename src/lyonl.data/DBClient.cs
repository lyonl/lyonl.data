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
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

// ReSharper disable once CheckNamespace
namespace LyonL.Data
{
    public abstract class DbClient<TException> : IDbClient where TException : Exception
    {
        private const int DefaultCommandTimeout = 30;
        private const int DefaultRetryCount = 0;
        private readonly List<CommandData> _commands = new List<CommandData>();

        private readonly Func<DbConnection> _connectionFactory;

        private readonly ParameterManager _parameterManager = new ParameterManager();
        private string _commandText;
        private int _commandTimeout = DefaultCommandTimeout;
        private CommandType _commandType = CommandType.Text;
        private int _retryCount = DefaultRetryCount;

        protected DbClient(Func<DbConnection> connectionFactory)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public IDbClient SetCommandText(string commandText)
        {
            _commandText = commandText;
            return this;
        }

        public IDbClient SetCommandTimeout(int timeout)
        {
            _commandTimeout = timeout;
            return this;
        }

        public IDbClient SetCommandType(CommandType commandType)
        {
            _commandType = commandType;
            return this;
        }

        public IDbClient SetRetryCount(int retryCount)
        {
            _retryCount = retryCount;
            return this;
        }

        public IDbClient AddDbParameter(IDataParameter parameter)
        {
            if (parameter == null) return this;

            _parameterManager.AddDbParameter(parameter);
            return this;
        }

        public IDbClient AddDbParameters(IEnumerable<IDataParameter> parameters)
        {
            parameters?.ToList().ForEach(_parameterManager.AddDbParameter);
            return this;
        }

        public IDbClient AddNamedParameters(object namedParameters, CrudMethod crudMethodType)
        {
            _parameterManager.AddNamedParameters(namedParameters, crudMethodType);
            return this;
        }

        public IDbClient AddNamedParameters(object namedParameters)
        {
            return AddNamedParameters(namedParameters, CrudMethod.None);
        }

        public IDbClient PushCommand()
        {
            _commands.Add(new CommandData
            {
                CommandText = _commandText,
                CommandTimeout = _commandTimeout,
                CommandType = _commandType,
                Parameters = _parameterManager.BuildDapperParameters()
            });

            // Clear command text and parameters
            _commandText = "";
            _parameterManager.Clear();
            return this;
        }

        public IEnumerable<T> ExecuteQuery<T>(CancellationToken cancellationToken, IsolationLevel isolationLevel)
        {
            if (!string.IsNullOrWhiteSpace(_commandText))
                PushCommand();
            if (_commands.Count > 1) throw new DbClientException("ExecuteQuery only supports a single command");

            try
            {
                return Retry(() =>
                {
                    using (var conn = _connectionFactory.Invoke())
                    {
                        IEnumerable<T> output;
                        conn.Open();
                        var transaction = conn.BeginTransaction(isolationLevel);
                        var cmd = new CommandDefinition(_commands[0].CommandText,
                            _commands[0].Parameters,
                            transaction,
                            _commands[0].CommandTimeout,
                            _commands[0].CommandType,
                            cancellationToken: cancellationToken);
                        try
                        {
                            output = conn.Query<T>(cmd);
                            _parameterManager.ExtractOutputParameters(_commands[0].Parameters);
                            transaction.Commit();
                        }
                        catch
                        {
                            transaction?.Rollback();
                            throw;
                        }

                        return output;
                    }
                }, cancellationToken);
            }
            finally
            {
                Reset();
            }
        }

        public IEnumerable<TReturn> ExecuteQuery<TConcrete, TReturn>(CancellationToken cancellationToken,
            IsolationLevel isolationLevel)
            where TConcrete : TReturn
        {
            var result = ExecuteQuery<TConcrete>(cancellationToken, isolationLevel);
            return result.Select(s => s).Cast<TReturn>();
        }

        public int ExecuteNonQuery(CancellationToken cancellationToken, IsolationLevel isolationLevel)
        {
            if (!string.IsNullOrWhiteSpace(_commandText))
                PushCommand();
            try
            {
                return Retry(() =>
                {
                    using (var conn = _connectionFactory.Invoke())
                    {
                        var returnValue = 0;
                        conn.Open();
                        var transaction = conn.BeginTransaction(isolationLevel);
                        try
                        {
                            foreach (var commandData in _commands)
                                returnValue = Retry(() =>
                                {
                                    var cmd = new CommandDefinition(commandData.CommandText,
                                        cancellationToken: cancellationToken,
                                        parameters: commandData.Parameters,
                                        commandTimeout: commandData.CommandTimeout,
                                        commandType: commandData.CommandType);
                                    var output = conn.Execute(cmd);
                                    _parameterManager.ExtractOutputParameters(commandData.Parameters);
                                    return output;
                                }, cancellationToken);
                            transaction.Commit();
                        }
                        catch
                        {
                            transaction?.Rollback();
                            throw;
                        }

                        return returnValue;
                    }
                }, cancellationToken);
            }
            finally
            {
                Reset();
            }
        }


        public async Task<IEnumerable<T>> ExecuteQueryAsync<T>(CancellationToken cancellationToken,
            IsolationLevel isolationLevel)
        {
            if (!string.IsNullOrWhiteSpace(_commandText))
                PushCommand();
            if (_commands.Count > 1) throw new DbClientException("ExecuteQuery only supports a single command");

            try
            {
                var output = await RetryAsync(async () =>
                {
                    using (var conn = _connectionFactory.Invoke())
                    {
                        IEnumerable<T> results;
                        await conn.OpenAsync(cancellationToken);
                        var transaction = conn.BeginTransaction(isolationLevel);
                        var cmd = new CommandDefinition(_commands[0].CommandText,
                            _commands[0].Parameters,
                            transaction,
                            _commands[0].CommandTimeout,
                            _commands[0].CommandType);
                        try
                        {
                            results = await conn.QueryAsync<T>(cmd).ConfigureAwait(false);
                            _parameterManager.ExtractOutputParameters(_commands[0].Parameters);
                            transaction.Commit();
                        }
                        catch
                        {
                            transaction?.Rollback();
                            throw;
                        }

                        return results;
                    }
                }, cancellationToken).ConfigureAwait(false);
                return output;
            }
            finally
            {
                Reset();
            }
        }

        public async Task<IEnumerable<TReturn>> ExecuteQueryAsync<TConcrete, TReturn>(
            CancellationToken cancellationToken, IsolationLevel isolationLevel) where TConcrete : TReturn
        {
            var result = await ExecuteQueryAsync<TConcrete>(cancellationToken, isolationLevel).ConfigureAwait(false);
            return result.Select(s => s).Cast<TReturn>();
        }

        public async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken, IsolationLevel isolationLevel)
        {
            // support multiple commands
            if (!string.IsNullOrWhiteSpace(_commandText))
                PushCommand();
            try
            {
                return await RetryAsync(async () =>
                {
                    using (var conn = _connectionFactory.Invoke())
                    {
                        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                        var transaction = conn.BeginTransaction(isolationLevel);
                        var returnValue = 0;
                        foreach (var commandData in _commands)
                        {
                            var cmd = new CommandDefinition(commandData.CommandText,
                                commandData.Parameters,
                                transaction,
                                commandData.CommandTimeout,
                                commandData.CommandType,
                                cancellationToken: cancellationToken);
                            try
                            {
                                returnValue = await conn.ExecuteAsync(cmd).ConfigureAwait(false);
                                _parameterManager.ExtractOutputParameters(commandData.Parameters);
                            }
                            catch
                            {
                                transaction?.Rollback();
                                throw;
                            }
                        }

                        transaction.Commit();
                        return returnValue;
                    }
                }, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                Reset();
            }
        }

        ~DbClient()
        {
            Dispose(false);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        private void Reset()
        {
            _commandText = null;
            _commandTimeout = DefaultCommandTimeout;
            _commandType = CommandType.Text;
            _parameterManager.Clear();
            _commands.Clear();
        }

        protected T Retry<T>(Func<T> func, CancellationToken cancelToken = default(CancellationToken))
        {
            var count = 0;
            while (true)
                try
                {
                    return func();
                }
                catch (TException ex)
                {
                    cancelToken.ThrowIfCancellationRequested();
                    if (++count > _retryCount || SqlRetry(ex)) throw;
                }
        }

        protected void Retry(Action func, CancellationToken cancelToken = default(CancellationToken))
        {
            var count = 0;
            while (true)
                try
                {
                    func();
                }
                catch (TException ex)
                {
                    cancelToken.ThrowIfCancellationRequested();
                    if (++count > _retryCount || SqlRetry(ex)) throw;
                }
        }

        protected async Task<T> RetryAsync<T>(Func<Task<T>> func,
            CancellationToken cancelToken = default(CancellationToken))
        {
            var count = 0;
            while (true)
                try
                {
                    return await func().ConfigureAwait(false);
                }
                catch (TException ex)
                {
                    cancelToken.ThrowIfCancellationRequested();
                    if (++count > _retryCount || SqlRetry(ex)) throw;
                }
        }

        protected async Task RetryAsync(Func<Task> func, CancellationToken cancelToken = default(CancellationToken))
        {
            var count = 0;
            while (true)
                try
                {
                    await func().ConfigureAwait(false);
                }
                catch (TException ex)
                {
                    cancelToken.ThrowIfCancellationRequested();
                    if (++count > _retryCount || SqlRetry(ex)) throw;
                }
        }

        protected abstract bool SqlRetry(TException ex);


        public async Task<IEnumerable<T>> ExecuteQueryCleanAsync<T>(CancellationToken cancellationToken,
            IsolationLevel isolationLevel)
        {
            if (!string.IsNullOrWhiteSpace(_commandText))
                PushCommand();
            if (_commands.Count > 1) throw new DbClientException("ExecuteQuery only supports a single command");

            try
            {
                using (var conn = _connectionFactory.Invoke())
                {
                    IEnumerable<T> results = null;
                    await conn.OpenAsync(cancellationToken);
                    var transaction = conn.BeginTransaction(isolationLevel);
                    var cmd = new CommandDefinition(_commands[0].CommandText,
                        _commands[0].Parameters,
                        transaction,
                        _commands[0].CommandTimeout,
                        _commands[0].CommandType);
                    try
                    {
                        results = await conn.QueryAsync<T>(cmd).ConfigureAwait(false);
                        _parameterManager.ExtractOutputParameters(_commands[0].Parameters);
                        transaction.Commit();
                    }
                    catch
                    {
                        transaction?.Rollback();
                        throw;
                    }

                    return results;
                }
            }
            finally
            {
                Reset();
            }
        }

        private class CommandData
        {
            public string CommandText { get; set; }
            public int CommandTimeout { get; set; }
            public CommandType CommandType { get; set; }
            public DynamicParameters Parameters { get; set; }
        }
    }
}