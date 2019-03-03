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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Dapper;
using LyonL.Data.Attributes;


// ReSharper disable once CheckNamespace
namespace LyonL.Data
{
    internal class ParameterManager
    {
        public ICollection<KeyValuePair<string, object>> NamedParameters { get; } =
            new HashSet<KeyValuePair<string, object>>();

        public ICollection<IDataParameter> DbParameters { get; } = new HashSet<IDataParameter>();

        public void AddDbParameter(IDataParameter parameter)
        {
            if (parameter == null) return;

            DbParameters.Add(parameter);
        }

        public void AddNamedParameters(object namedParameters, CrudMethod crudMethodType)
        {
            if (namedParameters == null) return;

            var ignoreAttribute = new List<Type> {typeof(IgnoreAttribute)};

            switch (crudMethodType)
            {
                case CrudMethod.Insert:
                    ignoreAttribute.Add(typeof(IgnoreOnInsertAttribute));
                    break;
                case CrudMethod.Update:
                    ignoreAttribute.Add(typeof(IgnoreOnUpdateAttribute));
                    break;
            }

            var props = namedParameters.GetType().GetProperties();
            foreach (var p in props)
            {
                var name = p.Name;
                var customAttributes = p.GetCustomAttributes(true);
                if (customAttributes.Any(x => x is ColumnAttribute))
                    name = ((ColumnAttribute) customAttributes.Single(x => x is ColumnAttribute)).Name;

                if (customAttributes.Any(x => ignoreAttribute.Contains(x.GetType()))) continue;

                var value = p.GetValue(namedParameters, null);
                if (DBNull.Value.Equals(value)) value = null;

                NamedParameters.Add(new KeyValuePair<string, object>(name, value));
            }
        }

        public void Clear()
        {
            DbParameters.Clear();
            NamedParameters.Clear();
        }

        public DynamicParameters BuildDapperParameters()
        {
            var dynamicParams = new DynamicParameters();
            if (NamedParameters != null) dynamicParams.AddDynamicParams(NamedParameters);

            DbParameters?.ToList().ForEach(p => dynamicParams.Add(p.ParameterName, p.Value, p.DbType, p.Direction));
            return dynamicParams;
        }

        // the complexity on this method is unavoidable, at least for the time being
        [SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
        public void ExtractOutputParameters(DynamicParameters mappedParameters)
        {
            DbParameters?.Where(
                    p => p.Direction == ParameterDirection.InputOutput || p.Direction == ParameterDirection.Output)
                .ToList().ForEach(p =>
                    {
                        switch (p.DbType)
                        {
                            case DbType.Int16:
                                p.Value = mappedParameters.Get<short>(p.ParameterName);
                                break;
                            case DbType.Int32:
                                p.Value = mappedParameters.Get<int>(p.ParameterName);
                                break;
                            case DbType.Int64:
                                p.Value = mappedParameters.Get<long>(p.ParameterName);
                                break;
                            case DbType.String:
                                p.Value = mappedParameters.Get<string>(p.ParameterName);
                                break;
                            case DbType.AnsiString:
                                p.Value = mappedParameters.Get<string>(p.ParameterName);
                                break;
                            case DbType.Byte:
                                p.Value = mappedParameters.Get<byte>(p.ParameterName);
                                break;
                            case DbType.Boolean:
                                p.Value = mappedParameters.Get<bool>(p.ParameterName);
                                break;
                            case DbType.Date:
                                p.Value = mappedParameters.Get<DateTime>(p.ParameterName);
                                break;
                            case DbType.DateTime:
                                p.Value = mappedParameters.Get<DateTime>(p.ParameterName);
                                break;
                            case DbType.Decimal:
                                p.Value = mappedParameters.Get<decimal>(p.ParameterName);
                                break;
                            case DbType.Double:
                                p.Value = mappedParameters.Get<double>(p.ParameterName);
                                break;
                            case DbType.Guid:
                                p.Value = mappedParameters.Get<Guid>(p.ParameterName);
                                break;
                            case DbType.Single:
                                p.Value = mappedParameters.Get<float>(p.ParameterName);
                                break;
                            case DbType.AnsiStringFixedLength:
                                p.Value = mappedParameters.Get<string>(p.ParameterName);
                                break;
                            case DbType.StringFixedLength:
                                p.Value = mappedParameters.Get<string>(p.ParameterName);
                                break;
                            case DbType.Xml:
                                p.Value = mappedParameters.Get<string>(p.ParameterName);
                                break;
                            case DbType.DateTimeOffset:
                                p.Value = mappedParameters.Get<DateTimeOffset>(p.ParameterName);
                                break;
                            // The following types are not currently supported
                            case DbType.SByte:
                            //p.Value = mappedParameters.Get<sbyte>(p.ParameterName);
                            //break;
                            case DbType.Time:
                            //p.Value = mappedParameters.Get<DateTime>(p.ParameterName);
                            //break;
                            case DbType.UInt16:
                            //	p.Value = mappedParameters.Get<ushort>(p.ParameterName);
                            //	break;
                            case DbType.UInt32:
                            //	p.Value = mappedParameters.Get<uint>(p.ParameterName);
                            //	break;
                            case DbType.UInt64:
                            //	p.Value = mappedParameters.Get<ulong>(p.ParameterName);
                            //	break;
                            case DbType.Currency:
                            case DbType.Binary:
                            case DbType.DateTime2:
                            case DbType.Object:
                            case DbType.VarNumeric:
                                throw new NotImplementedException();
                            default:
                                throw new ArgumentOutOfRangeException(
                                    FormattableString.Invariant(
                                        $"DbType {p.DbType} for {p.ParameterName} is not recognized"));
                        }
                    }
                );
        }
    }
}