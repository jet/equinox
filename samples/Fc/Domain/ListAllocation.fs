module ListAllocation

open System

type Service(maxListLen, allocators : Allocator.Service, allocations : Allocation.Service, lists : TicketList.Service, tickets : Ticket.Service) =

    member __.Commence(allocatorId, allocationId, tickets, transactionTimeout) : Async<_> = async {
        let cutoff = let now = DateTimeOffset.UtcNow in now.Add transactionTimeout
        let! state = allocators.Commence(allocatorId, allocationId, cutoff)
        // TODO cancel timed out conflicting work
        let! (_,state) = allocations.Sync(allocationId, Seq.empty, Allocation.Commence tickets)
        return state }

    member __.Read(allocationId) : Async<_> = async {
        let! (_,state) = allocations.Sync(allocationId, Seq.empty, Allocation.Command.Apply ([],[]))
        // TODO incorporate allocator state
        return state }

    member __.Cancel(allocatorId,allocationId) : Async<_> = async {
        let! (_,state) = allocations.Sync(allocationId, Seq.empty, Allocation.Command.Cancel)
        // TODO propagate to allocator with reason
        return state }
