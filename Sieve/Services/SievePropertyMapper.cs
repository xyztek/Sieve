﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Sieve.Models;

namespace Sieve.Services
{
    public class SievePropertyMapper
    {
        private readonly Dictionary<Type, ICollection<KeyValuePair<MemberInfo, ISievePropertyMetadata>>> _map
            = new Dictionary<Type, ICollection<KeyValuePair<MemberInfo, ISievePropertyMetadata>>>();

        public PropertyFluentApi<TEntity> Property<TEntity>(Expression<Func<TEntity, object>> expression)
        {
            if (!_map.ContainsKey(typeof(TEntity)))
            {
                _map.Add(typeof(TEntity), new List<KeyValuePair<MemberInfo, ISievePropertyMetadata>>());
            }

            return new PropertyFluentApi<TEntity>(this, expression);
        }

        public class PropertyFluentApi<TEntity>
        {
            private readonly SievePropertyMapper _sievePropertyMapper;
            private readonly MemberInfo _member;

            public PropertyFluentApi(SievePropertyMapper sievePropertyMapper, Expression<Func<TEntity, object>> expression)
            {
                _sievePropertyMapper = sievePropertyMapper;
                (_fullName, _member) = GetPropertyInfo(expression);
                _name = _fullName;
                _canFilter = false;
                _canSort = false;
            }

            private string _name;
            private readonly string _fullName;
            private bool _canFilter;
            private bool _canSort;

            public PropertyFluentApi<TEntity> CanFilter()
            {
                _canFilter = true;
                UpdateMap();
                return this;
            }

            public PropertyFluentApi<TEntity> CanSort()
            {
                _canSort = true;
                UpdateMap();
                return this;
            }

            public PropertyFluentApi<TEntity> HasName(string name)
            {
                _name = name;
                UpdateMap();
                return this;
            }

            private void UpdateMap()
            {
                var metadata = new SievePropertyMetadata()
                {
                    Name = _name,
                    FullName = _fullName,
                    CanFilter = _canFilter,
                    CanSort = _canSort
                };
                var pair = new KeyValuePair<MemberInfo, ISievePropertyMetadata>(_member, metadata);

                _sievePropertyMapper._map[typeof(TEntity)].Add(pair);
            }

            private static (string, MemberInfo) GetPropertyInfo(Expression<Func<TEntity, object>> exp)
            {
                if (!(exp.Body is MemberExpression body))
                {
                    var ubody = (UnaryExpression)exp.Body;
                    body = ubody.Operand as MemberExpression;
                }

                var member = body?.Member;
                var stack = new Stack<string>();
                while (body != null)
                {
                    stack.Push(body.Member.Name);
                    body = body.Expression as MemberExpression;
                }

                return (string.Join(".", stack.ToArray()), member);
            }
        }

        public (string, MemberInfo) FindProperty<TEntity>(
            bool canSortRequired,
            bool canFilterRequired,
            string name,
            bool isCaseSensitive)
        {
            try
            {
                var result = _map[typeof(TEntity)]
                    .FirstOrDefault(kv =>
                    kv.Value.Name.Equals(name, isCaseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase)
                    && (!canSortRequired || kv.Value.CanSort)
                    && (!canFilterRequired || kv.Value.CanFilter));

                return (result.Value?.FullName, result.Key);
            }
            catch (Exception ex) when (ex is KeyNotFoundException || ex is ArgumentNullException)
            {
                return (null, null);
            }
        }
    }
}
