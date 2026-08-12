using Xunit;

namespace SimpleEventStore.Tests;

public sealed class ReflectionEventApplierTests
{
    private static readonly RecordDetails Details = new(
        "stream",
        StreamVersion.First,
        RecordId.First,
        DateTimeOffset.UnixEpoch);

    [Fact]
    public void Most_specific_event_type_then_one_argument_overload_wins_deterministically()
    {
        var entity = new OverloadedEntity();

        new ReflectionEventApplier().Apply(new SpecificEvent(), Details, entity);

        Assert.Equal("specific-one", entity.Applied);
    }

    [Fact]
    public void Inherited_and_interface_handlers_are_discovered()
    {
        var applier = new ReflectionEventApplier();
        var inherited = new InheritedEntity();
        var byInterface = new InterfaceEntity();

        applier.Apply(new SpecificEvent(), Details, inherited);
        applier.Apply(new SpecificEvent(), Details, byInterface);

        Assert.Equal("base", inherited.Applied);
        Assert.Equal("interface", byInterface.Applied);
        Assert.Contains(typeof(IMarkerEvent), applier.GetHandledEventTypes(byInterface));
    }

    [Fact]
    public void Interface_handler_is_more_specific_than_root_event_handler()
    {
        var entity = new InterfaceVersusRootEntity();

        new ReflectionEventApplier().Apply(new SpecificEvent(), Details, entity);

        Assert.Equal("interface", entity.Applied);
    }

    [Fact]
    public void Two_argument_handler_receives_record_details()
    {
        var entity = new DetailsEntity();

        new ReflectionEventApplier().Apply(new SpecificEvent(), Details, entity);

        Assert.Same(Details, entity.Details);
    }

    [Fact]
    public void Missing_public_handler_is_a_no_op()
    {
        new ReflectionEventApplier().Apply(new SpecificEvent(), Details, new NoHandlerEntity());
    }

    public interface IMarkerEvent : IEvent;

    public class BaseEvent : IEvent;

    public sealed class SpecificEvent : BaseEvent, IMarkerEvent;

    private sealed class OverloadedEntity
    {
        public string? Applied { get; private set; }
        public void Apply(IEvent _) => Applied = "any";
        public void Apply(BaseEvent _) => Applied = "base";
        public void Apply(SpecificEvent _, RecordDetails __) => Applied = "specific-two";
        public void Apply(SpecificEvent _) => Applied = "specific-one";
    }

    private class BaseEntity
    {
        public string? Applied { get; protected set; }
        public void Apply(BaseEvent _) => Applied = "base";
    }

    private sealed class InheritedEntity : BaseEntity;

    private sealed class InterfaceEntity
    {
        public string? Applied { get; private set; }
        public void Apply(IMarkerEvent _) => Applied = "interface";
    }

    private sealed class InterfaceVersusRootEntity
    {
        public string? Applied { get; private set; }
        public void Apply(IEvent _) => Applied = "root";
        public void Apply(IMarkerEvent _) => Applied = "interface";
    }

    private sealed class DetailsEntity
    {
        public RecordDetails? Details { get; private set; }
        public void Apply(SpecificEvent _, RecordDetails details) => Details = details;
    }

    private sealed class NoHandlerEntity
    {
        private void Apply(SpecificEvent _) => throw new InvalidOperationException();
    }
}
