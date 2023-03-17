using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Sieve.Exceptions;

namespace Sieve.Extensions
{
    public static partial class LinqExtensions
    {
        private static bool IsExpressionProperty(this MemberExpression memberExpression)
        {
            var valueType = memberExpression.Member switch
            {
                PropertyInfo propertyInfo => propertyInfo.PropertyType,
                FieldInfo fieldInfo => fieldInfo.FieldType,
                _ => null
            };

            return valueType != null &&
                   typeof(Expression).IsAssignableFrom(valueType);
        }

        private static Expression GeneratePropertyAccess
        (
            this Expression expression,
            string propertyName
        )
        {
            try
            {
                Expression accessExpression = Expression.PropertyOrField(expression, propertyName);

                if
                (
                    expression is ParameterExpression parameterExpression &&
                    ((MemberExpression)accessExpression).IsExpressionProperty()
                )
                {
                    accessExpression = Expression.Invoke(accessExpression, parameterExpression);
                }

                return accessExpression;
            }
            catch (ArgumentException)
            {
                var possibleInterfaceType = expression.Type;

                if (!possibleInterfaceType.IsInterface)
                    return CheckStatic
                    (
                        expression,
                        propertyName
                    );

                return CheckInterface
                (
                    expression,
                    propertyName
                );
            }

            static Expression CheckStatic
            (
                Expression originalExpression,
                string propertyName
            )
            {
                var expressionType = originalExpression.Type;

                var members = expressionType
                    .GetMembers()
                    .Where(info => info.Name == propertyName)
                    .ToList();

                object memberValue = null;

                var propertyInfo = members
                    .OfType<PropertyInfo>()
                    .SingleOrDefault();

                if (propertyInfo != null && propertyInfo.GetMethod.IsStatic)
                {
                    memberValue = propertyInfo.GetValue(null);
                }
                else
                {
                    var fieldInfo = members
                        .OfType<FieldInfo>()
                        .SingleOrDefault();

                    if (fieldInfo != null && fieldInfo.IsStatic)
                        memberValue = fieldInfo.GetValue(null);
                }

                if (!(memberValue is LambdaExpression lambdaExpression)) 
                    return originalExpression;

                var valueExpression = Expression.Invoke(lambdaExpression, originalExpression);

                return valueExpression;
            }

            // propertyName is not a direct property of field of expression's type.
            // when expression.Type is directly an interface, say typeof(ISomeEntity) and ISomeEntity is interfaced to IBaseEntity
            // in which `propertyName` is defined in the first place.
            // To solve this, search `propertyName` in all other implemented interfaces
            static Expression CheckInterface
            (
                Expression originalExpression,
                string propertyName
            )
            {
                // get all implemented interface types
                var implementedInterfaces = originalExpression.Type.GetInterfaces();

                Expression interfacedExpression;
                try
                {
                    // search propertyName in all interfaces
                    interfacedExpression = implementedInterfaces
                        .Where
                        (
                            implementedInterface => implementedInterface
                                .GetProperties()
                                .Any(info => info.Name == propertyName)
                        )
                        .Select(implementedInterface => Expression.TypeAs(originalExpression, implementedInterface))
                        .Single();
                }
                catch (InvalidOperationException ioe)
                {
                    throw new SieveException
                    (
                        $"{propertyName} is repeated in interface hierarchy. Try renaming.",
                        ioe
                    );
                }

                try
                {
                    var accessExpression = interfacedExpression.GeneratePropertyAccess(propertyName);

                    return accessExpression;
                }
                catch (ArgumentException)
                {
                    // try to determine from targetMemberInfo.. Maybe it's a static Member

                    return CheckStatic
                    (
                        interfacedExpression,
                        propertyName
                    );
                }
            }
        }

        public static IQueryable<TEntity> OrderByDynamic<TEntity>
        (
            this IQueryable<TEntity> source,
            string fullPropertyName,
            bool desc,
            bool useThenBy,
            bool disableNullableTypeExpression = false
        )
        {
            var parameterExpression = Expression.Parameter(typeof(TEntity), "e");

            var lambda = parameterExpression
                .GenerateFullExpressionTree
                (
                    fullPropertyName,
                    false,
                    null,
                    disableNullableTypeExpression,
                    out var nullCheckExpression,
                    out _
                )
                .GenerateConditionalOrderByAccess(nullCheckExpression)
                .GenerateLambda<TEntity>(parameterExpression);

            var command = useThenBy
                ? "ThenBy"
                : "OrderBy";

            if (desc)
            {
                command = useThenBy
                    ? "ThenByDescending"
                    : "OrderByDescending";
            }

            var resultExpression = Expression.Call
            (
                typeof(Queryable),
                command,
                new Type[] { typeof(TEntity), lambda.ReturnType },
                source.Expression,
                Expression.Quote(lambda)
            );

            return source.Provider.CreateQuery<TEntity>(resultExpression);
        }

        private static Expression<Func<TEntity, object>> GenerateLambda<TEntity>
        (
            this Expression expression,
            ParameterExpression parameter
        )
        {
            var converted = Expression.Convert(expression, typeof(object));

            return Expression.Lambda<Func<TEntity, object>>(converted, parameter);
        }

        private static LambdaExpression GenerateLambda
        (
            this Expression expression,
            ParameterExpression parameter
        )
        {
            var converted = Expression.Convert(expression, typeof(object));

            return Expression.Lambda(converted, parameter);
        }

        private static Expression GenerateConditionalOrderByAccess
        (
            this Expression expression,
            Expression nullCheckExpression
        )
        {
            return nullCheckExpression == null
                ? expression
                : Expression.Condition
                (
                    nullCheckExpression,
                    Expression.Default(expression.Type),
                    expression
                );
        }

        public static Expression GenerateFullExpressionTree
        (
            this ParameterExpression entityParameterExpression,
            string fullPropertyName,
            bool forFilterOperation,
            bool? isFilterTermValueNull,
            bool? disableNullableTypeExpression,
            out Expression cumulativeNullCheckExpression,
            out Type dataType
        )
        {
            cumulativeNullCheckExpression = null;

            Expression headExpression = entityParameterExpression;
            var propertyNames = fullPropertyName.Split('.');

            for (var index = 0; index < propertyNames.Length; index++)
            {
                headExpression = headExpression.GenerateSafeMemberAccess
                (
                    propertyNames[index],
                    forFilterOperation,
                    isFilterTermValueNull,
                    forFilterOperation ? (bool?)(index == propertyNames.Length - 1) : null,
                    disableNullableTypeExpression,
                    ref cumulativeNullCheckExpression
                );
            }

            dataType = headExpression.Type;

            return headExpression;
        }

        private static Expression GenerateSafeMemberAccess
        (
            this Expression expression,
            string propertyName,
            bool forFilterOperation,
            bool? isFilterTermValueNull,
            bool? finalMember,
            bool? disableNullableTypeExpression,
            ref Expression cumulativeNullCheckExpression
        )
        {
            var accessExpression = expression.GeneratePropertyAccess(propertyName);

            cumulativeNullCheckExpression = accessExpression.GetNullCheckExpression
            (
                cumulativeNullCheckExpression,
                forFilterOperation,
                isFilterTermValueNull,
                finalMember,
                disableNullableTypeExpression
            );
            
            return accessExpression;
        }

        public static Expression GetNullCheckExpression
        (
            this Expression expression,
            Expression currentNullCheckExpression,
            bool forFilterOperation,
            bool? isFilterTermValueNull,
            bool? finalMember,
            bool? disableNullableTypeExpression
        )
        {
            if (!expression.Type.IsNullable())
                return currentNullCheckExpression;

            if (forFilterOperation)
            {
                if (isFilterTermValueNull.HasValue && !isFilterTermValueNull.Value)
                {
                    return GenerateNullCheckExpression
                    (
                        expression,
                        currentNullCheckExpression,
                        true
                    );
                }

                if(finalMember.HasValue && !finalMember.Value)
                    return GenerateNullCheckExpression
                    (
                        expression,
                        currentNullCheckExpression,
                        true
                    );
            }
            else
            {
                if (disableNullableTypeExpression.HasValue && !disableNullableTypeExpression.Value)
                {
                    return GenerateNullCheckExpression
                    (
                        expression,
                        currentNullCheckExpression,
                        false
                    );
                }
            }

            return currentNullCheckExpression;
        }

        private static Expression GenerateNullCheckExpression
        (
            this Expression expression,
            Expression currentNullCheckExpression,
            bool forFilter
        )
        {
            var defaultExpression = Expression.Default(expression.Type);
            var equalsExpression = Expression.Equal(expression, defaultExpression);
            var notEqualsExpression = Expression.NotEqual(expression, defaultExpression);

            if (forFilter)
                return currentNullCheckExpression == null
                    ? notEqualsExpression
                    : Expression.AndAlso
                    (
                        currentNullCheckExpression,
                        notEqualsExpression
                    );

            return currentNullCheckExpression == null
                ? equalsExpression
                : Expression.OrElse
                (
                    currentNullCheckExpression,
                    equalsExpression
                );
        }
    }
}
