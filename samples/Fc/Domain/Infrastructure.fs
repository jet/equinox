namespace global

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings

type LocationId = string<locationId>
and [<Measure>] locationId
module LocationId =
    let parse (value : string) : LocationId = %value
    let toString (value : LocationId) : string = %value

type LocationEpochId = int<locationEpochId>
and [<Measure>] locationEpochId
module LocationEpochId =
    let parse (value : int) : LocationEpochId = %value
    let next (value : LocationEpochId) : LocationEpochId = % (%value + 1)
    let toString (value : LocationEpochId) : string = string %value

type TicketId = string<ticketId>
and [<Measure>] ticketId
module TicketId =
    let parse (value : string) : TicketId = let raw = value in %raw
    let toString (value : TicketId) : string = %value

type TicketListId = string<ticketListId>
and [<Measure>] ticketListId
module PickListId =
    let parse (value : string) : TicketListId = let raw = value in %raw
    let toString (value : TicketListId) : string = %value

type AllocationId = string<allocationId>
and [<Measure>] allocationId
module AllocationId =
    let parse (value : string) : AllocationId = let raw = value in %raw
    let toString (value : AllocationId) : string = %value

type AllocatorId = string<allocatorId>
and [<Measure>] allocatorId
module AllocatorId =
    let parse (value : string) : AllocatorId = let raw = value in %raw
    let toString (value : AllocatorId) : string = %value