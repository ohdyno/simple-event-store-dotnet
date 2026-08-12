using System.Reflection;
using System.Runtime.ExceptionServices;

namespace SimpleEventStore;

/// <summary>Discovers and invokes compatible public <c>Apply</c> methods.</summary>
public sealed class ReflectionEventApplier : IEventApplier
{
    public void Apply(IEvent eventValue, RecordDetails details, object entity)
    {
        ArgumentNullException.ThrowIfNull(eventValue);
        ArgumentNullException.ThrowIfNull(details);
        ArgumentNullException.ThrowIfNull(entity);

        var eventType = eventValue.GetType();
        var candidates = GetApplyMethods(entity.GetType())
            .Where(candidate => candidate.EventType.IsAssignableFrom(eventType))
            .ToArray();
        var method = candidates
            .OrderByDescending(candidate => candidates.Count(other =>
                other.EventType != candidate.EventType
                && other.EventType.IsAssignableFrom(candidate.EventType)))
            .ThenBy(candidate => GetTypeDistance(eventType, candidate.EventType))
            .ThenBy(candidate => candidate.Method.GetParameters().Length)
            .ThenBy(candidate => candidate.EventType.FullName, StringComparer.Ordinal)
            .ThenBy(candidate => candidate.Method.DeclaringType?.FullName, StringComparer.Ordinal)
            .Select(candidate => candidate.Method)
            .FirstOrDefault();

        if (method is null)
        {
            return;
        }

        try
        {
            var arguments = method.GetParameters().Length == 1
                ? new object[] { eventValue }
                : new object[] { eventValue, details };
            method.Invoke(entity, arguments);
        }
        catch (TargetInvocationException exception) when (exception.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
        }
    }

    public IReadOnlyList<Type> GetHandledEventTypes(object entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        return GetApplyMethods(entity.GetType())
            .Select(static method => method.EventType)
            .Distinct()
            .OrderBy(static type => type.FullName, StringComparer.Ordinal)
            .ToArray();
    }

    private static IEnumerable<(MethodInfo Method, Type EventType)> GetApplyMethods(Type entityType) =>
        entityType.GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .Where(static method =>
            {
                if (!string.Equals(method.Name, "Apply", StringComparison.Ordinal) || method.IsGenericMethod)
                {
                    return false;
                }

                var parameters = method.GetParameters();
                return parameters.Length is 1 or 2
                    && typeof(IEvent).IsAssignableFrom(parameters[0].ParameterType)
                    && (parameters.Length == 1 || parameters[1].ParameterType == typeof(RecordDetails));
            })
            .Select(static method => (method, method.GetParameters()[0].ParameterType));

    private static int GetTypeDistance(Type concreteType, Type targetType)
    {
        if (concreteType == targetType)
        {
            return 0;
        }

        var seen = new HashSet<Type> { concreteType };
        var queue = new Queue<(Type Type, int Distance)>();
        queue.Enqueue((concreteType, 0));
        while (queue.TryDequeue(out var current))
        {
            var adjacent = current.Type.GetInterfaces().AsEnumerable();
            if (current.Type.BaseType is { } baseType)
            {
                adjacent = adjacent.Append(baseType);
            }

            foreach (var type in adjacent)
            {
                if (!seen.Add(type))
                {
                    continue;
                }

                if (type == targetType)
                {
                    return current.Distance + 1;
                }

                queue.Enqueue((type, current.Distance + 1));
            }
        }

        return int.MaxValue;
    }
}
