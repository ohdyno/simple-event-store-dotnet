using SimpleEventStore.ContractTests;

namespace SimpleEventStore.Tests;

public sealed class InMemoryStorageContractTests : StorageContractTests
{
    protected override IEventStorage Storage { get; } = new InMemoryEventStorage();
}
