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
    let toString (value : TicketId) : string = %value
    let parse (value : string) : TicketId = let raw = value in %raw

type TicketListId = string<ticketListId>
and [<Measure>] ticketListId
module PickListId =
    let toString (value : TicketListId) : string = %value
    let parse (value : string) : TicketListId = let raw = value in %raw

type TicketAllocatorId = string<ticketAllocatorId>
and [<Measure>] ticketAllocatorId
module TicketAllocatorId =
    let toString (value : TicketAllocatorId) : string = %value