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

type TicketTransId = string<ticketTransId>
and [<Measure>] ticketTransId
module TicketTransId =
    let parse (value : string) : TicketTransId = let raw = value in %raw
    let toString (value : TicketTransId) : string = %value

type TicketAllocatorId = string<ticketAllocatorId>
and [<Measure>] ticketAllocatorId
module TicketAllocatorId =
    let parse (value : string) : TicketAllocatorId = let raw = value in %raw
    let toString (value : TicketAllocatorId) : string = %value