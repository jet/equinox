module Fc.ListTicketSaga

open System.Threading

type Result =
    | Ok of owned : PickTicketId list
    | Conflict of PickList.BusyState

type Service(listService : PickList.Service, ticketService : PickTicket.Service) =
    static let maxDop = new SemaphoreSlim 10

    let release (transactionId,listId,tickets) = async {
        let! results = seq { for x in tickets do yield ticketService.Sync(x,transactionId,None) |> maxDop.Throttle } |> Async.Parallel
        let removed,conflicted = results |> Array.partition (function PickTicket.Result.Ok -> true | PickTicket.Result.Conflict _ -> false)
        return removed }

    let acquire (transactionId,listId,tickets) = async {
        let! results = seq { for x in tickets do yield ticketService.Sync(x,transactionId,Some listId) |> maxDop.Throttle } |> Async.Parallel
        let added,conflicted = results |> Array.partition (function PickTicket.Result.Ok -> true | PickTicket.Result.Conflict _ -> false)
        return added }

    let rec sync attempts (transactionId, listId, tickets, removed, acquired) : Async<Result> = async {
        match! listService.Sync(listId, transactionId, tickets, removed, acquired) with
        | PickList.Result.Conflict state ->
            return Conflict state
        | PickList.Result.Ok state ->
            if attempts = 0 then
                return Ok state.owned
            else
                let! removed =
                    match state.toRelease with
                    | [] -> async { return [] }
                    | rel -> release (transactionId, listId, rel)
                let! acquired =
                    match state.toAcquire with
                    | [] -> async { return [] }
                    | acq -> acquire (transactionId, listId, acq)
                match removed, acquired with
                | [], [] -> return Ok state.owned
                | rel, acq -> return! sync (attempts-1) (transactionId, listId, tickets, removed, acquired)
    }

    member __.Allocate(transactionId, listId, tickets) =
        sync 2 (transactionId, listId, tickets, [], [])

    /// Triggered by a reactor when there's been a Commenced without a Completed or Aborted
    member __.Deallocate(transactionId, listId) =
        sync 0 (transactionId, listId, [], [], [])
        // TODO think
