---
title: Protocol Buffer API
description: Auto-generated documentation for Angzarr protobuf definitions
---

# Protocol Documentation

## Table of Contents {#top}

- [angzarr_client/proto/angzarr/cloudevents.proto](#angzarr_client_proto_angzarr_cloudevents-proto)
    - [CloudEvent](#angzarr_client-proto-angzarr-CloudEvent)
    - [CloudEvent.ExtensionsEntry](#angzarr_client-proto-angzarr-CloudEvent-ExtensionsEntry)
    - [CloudEventsResponse](#angzarr_client-proto-angzarr-CloudEventsResponse)
  
- [angzarr_client/proto/angzarr/command_handler.proto](#angzarr_client_proto_angzarr_command_handler-proto)
    - [BusinessResponse](#angzarr_client-proto-angzarr-BusinessResponse)
    - [CommandResponse](#angzarr_client-proto-angzarr-CommandResponse)
    - [FactInjectionResponse](#angzarr_client-proto-angzarr-FactInjectionResponse)
    - [FactRequest](#angzarr_client-proto-angzarr-FactRequest)
    - [ReplayRequest](#angzarr_client-proto-angzarr-ReplayRequest)
    - [ReplayResponse](#angzarr_client-proto-angzarr-ReplayResponse)
    - [RevocationResponse](#angzarr_client-proto-angzarr-RevocationResponse)
    - [SpeculateCommandHandlerRequest](#angzarr_client-proto-angzarr-SpeculateCommandHandlerRequest)
  
    - [CommandHandlerCoordinatorService](#angzarr_client-proto-angzarr-CommandHandlerCoordinatorService)
    - [CommandHandlerService](#angzarr_client-proto-angzarr-CommandHandlerService)
  
- [angzarr_client/proto/angzarr/meta.proto](#angzarr_client_proto_angzarr_meta-proto)
    - [DeleteEditionEvents](#angzarr_client-proto-angzarr-DeleteEditionEvents)
  
- [angzarr_client/proto/angzarr/process_manager.proto](#angzarr_client_proto_angzarr_process_manager-proto)
    - [ProcessManagerCoordinatorRequest](#angzarr_client-proto-angzarr-ProcessManagerCoordinatorRequest)
    - [ProcessManagerHandleRequest](#angzarr_client-proto-angzarr-ProcessManagerHandleRequest)
    - [ProcessManagerHandleRequest.DestinationSequencesEntry](#angzarr_client-proto-angzarr-ProcessManagerHandleRequest-DestinationSequencesEntry)
    - [ProcessManagerHandleResponse](#angzarr_client-proto-angzarr-ProcessManagerHandleResponse)
    - [SpeculatePmRequest](#angzarr_client-proto-angzarr-SpeculatePmRequest)
  
    - [ProcessManagerCoordinatorService](#angzarr_client-proto-angzarr-ProcessManagerCoordinatorService)
    - [ProcessManagerService](#angzarr_client-proto-angzarr-ProcessManagerService)
  
- [angzarr_client/proto/angzarr/projector.proto](#angzarr_client_proto_angzarr_projector-proto)
    - [SpeculateProjectorRequest](#angzarr_client-proto-angzarr-SpeculateProjectorRequest)
  
    - [ProjectorCoordinatorService](#angzarr_client-proto-angzarr-ProjectorCoordinatorService)
    - [ProjectorService](#angzarr_client-proto-angzarr-ProjectorService)
  
- [angzarr_client/proto/angzarr/query.proto](#angzarr_client_proto_angzarr_query-proto)
    - [EventQueryService](#angzarr_client-proto-angzarr-EventQueryService)
  
- [angzarr_client/proto/angzarr/saga.proto](#angzarr_client_proto_angzarr_saga-proto)
    - [SagaCompensationFailed](#angzarr_client-proto-angzarr-SagaCompensationFailed)
    - [SagaHandleRequest](#angzarr_client-proto-angzarr-SagaHandleRequest)
    - [SagaHandleRequest.DestinationSequencesEntry](#angzarr_client-proto-angzarr-SagaHandleRequest-DestinationSequencesEntry)
    - [SagaResponse](#angzarr_client-proto-angzarr-SagaResponse)
    - [SpeculateSagaRequest](#angzarr_client-proto-angzarr-SpeculateSagaRequest)
  
    - [SagaCoordinatorService](#angzarr_client-proto-angzarr-SagaCoordinatorService)
    - [SagaService](#angzarr_client-proto-angzarr-SagaService)
  
- [angzarr_client/proto/angzarr/stream.proto](#angzarr_client_proto_angzarr_stream-proto)
    - [EventStreamService](#angzarr_client-proto-angzarr-EventStreamService)
  
- [angzarr_client/proto/angzarr/types.proto](#angzarr_client_proto_angzarr_types-proto)
    - [AggregateRoot](#angzarr_client-proto-angzarr-AggregateRoot)
    - [AngzarrDeadLetter](#angzarr_client-proto-angzarr-AngzarrDeadLetter)
    - [AngzarrDeadLetter.MetadataEntry](#angzarr_client-proto-angzarr-AngzarrDeadLetter-MetadataEntry)
    - [AngzarrDeferredSequence](#angzarr_client-proto-angzarr-AngzarrDeferredSequence)
    - [CascadeCommit](#angzarr_client-proto-angzarr-CascadeCommit)
    - [CascadeConflictDetail](#angzarr_client-proto-angzarr-CascadeConflictDetail)
    - [CascadeRollback](#angzarr_client-proto-angzarr-CascadeRollback)
    - [CommandBook](#angzarr_client-proto-angzarr-CommandBook)
    - [CommandPage](#angzarr_client-proto-angzarr-CommandPage)
    - [CommandRequest](#angzarr_client-proto-angzarr-CommandRequest)
    - [Compensate](#angzarr_client-proto-angzarr-Compensate)
    - [ComponentDescriptor](#angzarr_client-proto-angzarr-ComponentDescriptor)
    - [Confirmation](#angzarr_client-proto-angzarr-Confirmation)
    - [ContextualCommand](#angzarr_client-proto-angzarr-ContextualCommand)
    - [ContextualCommandRequest](#angzarr_client-proto-angzarr-ContextualCommandRequest)
    - [Cover](#angzarr_client-proto-angzarr-Cover)
    - [DomainDivergence](#angzarr_client-proto-angzarr-DomainDivergence)
    - [Edition](#angzarr_client-proto-angzarr-Edition)
    - [EventBook](#angzarr_client-proto-angzarr-EventBook)
    - [EventPage](#angzarr_client-proto-angzarr-EventPage)
    - [EventProcessingFailedDetails](#angzarr_client-proto-angzarr-EventProcessingFailedDetails)
    - [EventRequest](#angzarr_client-proto-angzarr-EventRequest)
    - [EventStreamFilter](#angzarr_client-proto-angzarr-EventStreamFilter)
    - [ExternalDeferredSequence](#angzarr_client-proto-angzarr-ExternalDeferredSequence)
    - [GetDescriptorRequest](#angzarr_client-proto-angzarr-GetDescriptorRequest)
    - [NoOp](#angzarr_client-proto-angzarr-NoOp)
    - [Notification](#angzarr_client-proto-angzarr-Notification)
    - [PageHeader](#angzarr_client-proto-angzarr-PageHeader)
    - [PayloadReference](#angzarr_client-proto-angzarr-PayloadReference)
    - [PayloadRetrievalFailedDetails](#angzarr_client-proto-angzarr-PayloadRetrievalFailedDetails)
    - [Projection](#angzarr_client-proto-angzarr-Projection)
    - [Query](#angzarr_client-proto-angzarr-Query)
    - [RejectionNotification](#angzarr_client-proto-angzarr-RejectionNotification)
    - [Revocation](#angzarr_client-proto-angzarr-Revocation)
    - [SequenceMismatchDetails](#angzarr_client-proto-angzarr-SequenceMismatchDetails)
    - [SequenceRange](#angzarr_client-proto-angzarr-SequenceRange)
    - [SequenceSet](#angzarr_client-proto-angzarr-SequenceSet)
    - [Snapshot](#angzarr_client-proto-angzarr-Snapshot)
    - [Target](#angzarr_client-proto-angzarr-Target)
    - [TemporalQuery](#angzarr_client-proto-angzarr-TemporalQuery)
    - [UUID](#angzarr_client-proto-angzarr-UUID)
  
    - [CascadeErrorMode](#angzarr_client-proto-angzarr-CascadeErrorMode)
    - [MergeStrategy](#angzarr_client-proto-angzarr-MergeStrategy)
    - [PayloadStorageType](#angzarr_client-proto-angzarr-PayloadStorageType)
    - [SnapshotRetention](#angzarr_client-proto-angzarr-SnapshotRetention)
    - [SyncMode](#angzarr_client-proto-angzarr-SyncMode)
  
- [angzarr_client/proto/angzarr/upcaster.proto](#angzarr_client_proto_angzarr_upcaster-proto)
    - [UpcastRequest](#angzarr_client-proto-angzarr-UpcastRequest)
    - [UpcastResponse](#angzarr_client-proto-angzarr-UpcastResponse)
  
    - [UpcasterService](#angzarr_client-proto-angzarr-UpcasterService)
  
- [angzarr_client/proto/examples/ai_sidecar.proto](#angzarr_client_proto_examples_ai_sidecar-proto)
    - [ActionContext](#angzarr_client-proto-examples-ActionContext)
    - [ActionHistory](#angzarr_client-proto-examples-ActionHistory)
    - [ActionRequest](#angzarr_client-proto-examples-ActionRequest)
    - [ActionResponse](#angzarr_client-proto-examples-ActionResponse)
    - [BatchActionRequest](#angzarr_client-proto-examples-BatchActionRequest)
    - [BatchActionResponse](#angzarr_client-proto-examples-BatchActionResponse)
    - [EndSessionRequest](#angzarr_client-proto-examples-EndSessionRequest)
    - [EndSessionResponse](#angzarr_client-proto-examples-EndSessionResponse)
    - [Experience](#angzarr_client-proto-examples-Experience)
    - [HandEvent](#angzarr_client-proto-examples-HandEvent)
    - [HealthRequest](#angzarr_client-proto-examples-HealthRequest)
    - [HealthResponse](#angzarr_client-proto-examples-HealthResponse)
    - [OpponentProfile](#angzarr_client-proto-examples-OpponentProfile)
    - [OpponentQuery](#angzarr_client-proto-examples-OpponentQuery)
    - [OpponentStats](#angzarr_client-proto-examples-OpponentStats)
    - [OpponentStatsResponse](#angzarr_client-proto-examples-OpponentStatsResponse)
    - [RecordResponse](#angzarr_client-proto-examples-RecordResponse)
    - [ReloadModelRequest](#angzarr_client-proto-examples-ReloadModelRequest)
    - [ReloadModelResponse](#angzarr_client-proto-examples-ReloadModelResponse)
    - [StartSessionRequest](#angzarr_client-proto-examples-StartSessionRequest)
    - [StartSessionResponse](#angzarr_client-proto-examples-StartSessionResponse)
  
    - [AiSidecar](#angzarr_client-proto-examples-AiSidecar)
  
- [angzarr_client/proto/examples/buy_in.proto](#angzarr_client_proto_examples_buy_in-proto)
    - [BuyInCompleted](#angzarr_client-proto-examples-BuyInCompleted)
    - [BuyInConfirmed](#angzarr_client-proto-examples-BuyInConfirmed)
    - [BuyInFailed](#angzarr_client-proto-examples-BuyInFailed)
    - [BuyInInitiated](#angzarr_client-proto-examples-BuyInInitiated)
    - [BuyInOrchestratorState](#angzarr_client-proto-examples-BuyInOrchestratorState)
    - [BuyInPhaseChanged](#angzarr_client-proto-examples-BuyInPhaseChanged)
    - [BuyInRequested](#angzarr_client-proto-examples-BuyInRequested)
    - [BuyInReservationReleased](#angzarr_client-proto-examples-BuyInReservationReleased)
    - [ConfirmBuyIn](#angzarr_client-proto-examples-ConfirmBuyIn)
    - [InitiateBuyIn](#angzarr_client-proto-examples-InitiateBuyIn)
    - [PlayerSeated](#angzarr_client-proto-examples-PlayerSeated)
    - [ReleaseBuyIn](#angzarr_client-proto-examples-ReleaseBuyIn)
    - [SeatPlayer](#angzarr_client-proto-examples-SeatPlayer)
    - [SeatingRejected](#angzarr_client-proto-examples-SeatingRejected)
  
- [angzarr_client/proto/examples/hand.proto](#angzarr_client_proto_examples_hand-proto)
    - [ActionTaken](#angzarr_client-proto-examples-ActionTaken)
    - [AwardPot](#angzarr_client-proto-examples-AwardPot)
    - [BettingRoundComplete](#angzarr_client-proto-examples-BettingRoundComplete)
    - [BlindPosted](#angzarr_client-proto-examples-BlindPosted)
    - [CardsDealt](#angzarr_client-proto-examples-CardsDealt)
    - [CardsMucked](#angzarr_client-proto-examples-CardsMucked)
    - [CardsRevealed](#angzarr_client-proto-examples-CardsRevealed)
    - [CommunityCardsDealt](#angzarr_client-proto-examples-CommunityCardsDealt)
    - [DealCards](#angzarr_client-proto-examples-DealCards)
    - [DealCommunityCards](#angzarr_client-proto-examples-DealCommunityCards)
    - [DrawCompleted](#angzarr_client-proto-examples-DrawCompleted)
    - [HandComplete](#angzarr_client-proto-examples-HandComplete)
    - [HandState](#angzarr_client-proto-examples-HandState)
    - [PlayerAction](#angzarr_client-proto-examples-PlayerAction)
    - [PlayerHandState](#angzarr_client-proto-examples-PlayerHandState)
    - [PlayerHoleCards](#angzarr_client-proto-examples-PlayerHoleCards)
    - [PlayerInHand](#angzarr_client-proto-examples-PlayerInHand)
    - [PlayerStackSnapshot](#angzarr_client-proto-examples-PlayerStackSnapshot)
    - [PlayerTimedOut](#angzarr_client-proto-examples-PlayerTimedOut)
    - [PostBlind](#angzarr_client-proto-examples-PostBlind)
    - [PotAward](#angzarr_client-proto-examples-PotAward)
    - [PotAwarded](#angzarr_client-proto-examples-PotAwarded)
    - [PotWinner](#angzarr_client-proto-examples-PotWinner)
    - [RequestDraw](#angzarr_client-proto-examples-RequestDraw)
    - [RevealCards](#angzarr_client-proto-examples-RevealCards)
    - [ShowdownStarted](#angzarr_client-proto-examples-ShowdownStarted)
  
- [angzarr_client/proto/examples/orchestration.proto](#angzarr_client_proto_examples_orchestration-proto)
    - [OrchestrationFailure](#angzarr_client-proto-examples-OrchestrationFailure)
  
    - [BuyInPhase](#angzarr_client-proto-examples-BuyInPhase)
    - [RebuyPhase](#angzarr_client-proto-examples-RebuyPhase)
    - [RegistrationPhase](#angzarr_client-proto-examples-RegistrationPhase)
  
- [angzarr_client/proto/examples/player.proto](#angzarr_client_proto_examples_player-proto)
    - [ActionRequested](#angzarr_client-proto-examples-ActionRequested)
    - [DeductReservedFunds](#angzarr_client-proto-examples-DeductReservedFunds)
    - [DepositFunds](#angzarr_client-proto-examples-DepositFunds)
    - [FundsDeducted](#angzarr_client-proto-examples-FundsDeducted)
    - [FundsDeposited](#angzarr_client-proto-examples-FundsDeposited)
    - [FundsReleased](#angzarr_client-proto-examples-FundsReleased)
    - [FundsReserved](#angzarr_client-proto-examples-FundsReserved)
    - [FundsTransferred](#angzarr_client-proto-examples-FundsTransferred)
    - [FundsWithdrawn](#angzarr_client-proto-examples-FundsWithdrawn)
    - [PlayerRegistered](#angzarr_client-proto-examples-PlayerRegistered)
    - [PlayerReturningToPlay](#angzarr_client-proto-examples-PlayerReturningToPlay)
    - [PlayerSittingOut](#angzarr_client-proto-examples-PlayerSittingOut)
    - [PlayerState](#angzarr_client-proto-examples-PlayerState)
    - [PlayerState.TableReservationsEntry](#angzarr_client-proto-examples-PlayerState-TableReservationsEntry)
    - [RegisterPlayer](#angzarr_client-proto-examples-RegisterPlayer)
    - [ReleaseFunds](#angzarr_client-proto-examples-ReleaseFunds)
    - [RequestAction](#angzarr_client-proto-examples-RequestAction)
    - [ReserveFunds](#angzarr_client-proto-examples-ReserveFunds)
    - [SitIn](#angzarr_client-proto-examples-SitIn)
    - [SitOut](#angzarr_client-proto-examples-SitOut)
    - [TransferFunds](#angzarr_client-proto-examples-TransferFunds)
    - [WithdrawFunds](#angzarr_client-proto-examples-WithdrawFunds)
  
- [angzarr_client/proto/examples/poker_types.proto](#angzarr_client_proto_examples_poker_types-proto)
    - [Card](#angzarr_client-proto-examples-Card)
    - [Currency](#angzarr_client-proto-examples-Currency)
    - [HandRanking](#angzarr_client-proto-examples-HandRanking)
    - [Pot](#angzarr_client-proto-examples-Pot)
    - [Seat](#angzarr_client-proto-examples-Seat)
  
    - [ActionType](#angzarr_client-proto-examples-ActionType)
    - [BettingPhase](#angzarr_client-proto-examples-BettingPhase)
    - [GameVariant](#angzarr_client-proto-examples-GameVariant)
    - [HandRankType](#angzarr_client-proto-examples-HandRankType)
    - [PlayerType](#angzarr_client-proto-examples-PlayerType)
    - [Rank](#angzarr_client-proto-examples-Rank)
    - [Suit](#angzarr_client-proto-examples-Suit)
  
- [angzarr_client/proto/examples/rebuy.proto](#angzarr_client_proto_examples_rebuy-proto)
    - [AddRebuyChips](#angzarr_client-proto-examples-AddRebuyChips)
    - [ConfirmRebuyFee](#angzarr_client-proto-examples-ConfirmRebuyFee)
    - [InitiateRebuy](#angzarr_client-proto-examples-InitiateRebuy)
    - [RebuyChipsAdded](#angzarr_client-proto-examples-RebuyChipsAdded)
    - [RebuyCompleted](#angzarr_client-proto-examples-RebuyCompleted)
    - [RebuyFailed](#angzarr_client-proto-examples-RebuyFailed)
    - [RebuyFeeConfirmed](#angzarr_client-proto-examples-RebuyFeeConfirmed)
    - [RebuyFeeReleased](#angzarr_client-proto-examples-RebuyFeeReleased)
    - [RebuyInitiated](#angzarr_client-proto-examples-RebuyInitiated)
    - [RebuyOrchestratorState](#angzarr_client-proto-examples-RebuyOrchestratorState)
    - [RebuyPhaseChanged](#angzarr_client-proto-examples-RebuyPhaseChanged)
    - [RebuyRequested](#angzarr_client-proto-examples-RebuyRequested)
    - [ReleaseRebuyFee](#angzarr_client-proto-examples-ReleaseRebuyFee)
  
- [angzarr_client/proto/examples/registration.proto](#angzarr_client_proto_examples_registration-proto)
    - [ConfirmRegistrationFee](#angzarr_client-proto-examples-ConfirmRegistrationFee)
    - [InitiateTournamentRegistration](#angzarr_client-proto-examples-InitiateTournamentRegistration)
    - [RegistrationCompleted](#angzarr_client-proto-examples-RegistrationCompleted)
    - [RegistrationFailed](#angzarr_client-proto-examples-RegistrationFailed)
    - [RegistrationFeeConfirmed](#angzarr_client-proto-examples-RegistrationFeeConfirmed)
    - [RegistrationFeeReleased](#angzarr_client-proto-examples-RegistrationFeeReleased)
    - [RegistrationInitiated](#angzarr_client-proto-examples-RegistrationInitiated)
    - [RegistrationOrchestratorState](#angzarr_client-proto-examples-RegistrationOrchestratorState)
    - [RegistrationPhaseChanged](#angzarr_client-proto-examples-RegistrationPhaseChanged)
    - [RegistrationRequested](#angzarr_client-proto-examples-RegistrationRequested)
    - [ReleaseRegistrationFee](#angzarr_client-proto-examples-ReleaseRegistrationFee)
  
- [angzarr_client/proto/examples/table.proto](#angzarr_client_proto_examples_table-proto)
    - [AddChips](#angzarr_client-proto-examples-AddChips)
    - [ChipsAdded](#angzarr_client-proto-examples-ChipsAdded)
    - [CreateTable](#angzarr_client-proto-examples-CreateTable)
    - [EndHand](#angzarr_client-proto-examples-EndHand)
    - [HandEnded](#angzarr_client-proto-examples-HandEnded)
    - [HandEnded.StackChangesEntry](#angzarr_client-proto-examples-HandEnded-StackChangesEntry)
    - [HandStarted](#angzarr_client-proto-examples-HandStarted)
    - [JoinTable](#angzarr_client-proto-examples-JoinTable)
    - [LeaveTable](#angzarr_client-proto-examples-LeaveTable)
    - [PlayerJoined](#angzarr_client-proto-examples-PlayerJoined)
    - [PlayerLeft](#angzarr_client-proto-examples-PlayerLeft)
    - [PlayerSatIn](#angzarr_client-proto-examples-PlayerSatIn)
    - [PlayerSatOut](#angzarr_client-proto-examples-PlayerSatOut)
    - [PotResult](#angzarr_client-proto-examples-PotResult)
    - [SeatSnapshot](#angzarr_client-proto-examples-SeatSnapshot)
    - [StartHand](#angzarr_client-proto-examples-StartHand)
    - [TableCreated](#angzarr_client-proto-examples-TableCreated)
    - [TableState](#angzarr_client-proto-examples-TableState)
  
- [angzarr_client/proto/examples/tournament.proto](#angzarr_client_proto_examples_tournament-proto)
    - [AddonConfig](#angzarr_client-proto-examples-AddonConfig)
    - [AddonProcessed](#angzarr_client-proto-examples-AddonProcessed)
    - [AdvanceBlindLevel](#angzarr_client-proto-examples-AdvanceBlindLevel)
    - [BlindLevel](#angzarr_client-proto-examples-BlindLevel)
    - [BlindLevelAdvanced](#angzarr_client-proto-examples-BlindLevelAdvanced)
    - [CloseRegistration](#angzarr_client-proto-examples-CloseRegistration)
    - [CompleteTournament](#angzarr_client-proto-examples-CompleteTournament)
    - [CreateTournament](#angzarr_client-proto-examples-CreateTournament)
    - [EliminatePlayer](#angzarr_client-proto-examples-EliminatePlayer)
    - [EnrollPlayer](#angzarr_client-proto-examples-EnrollPlayer)
    - [OpenRegistration](#angzarr_client-proto-examples-OpenRegistration)
    - [PauseTournament](#angzarr_client-proto-examples-PauseTournament)
    - [PlayerEliminated](#angzarr_client-proto-examples-PlayerEliminated)
    - [PlayerRegistration](#angzarr_client-proto-examples-PlayerRegistration)
    - [PlayerUnregistered](#angzarr_client-proto-examples-PlayerUnregistered)
    - [ProcessAddon](#angzarr_client-proto-examples-ProcessAddon)
    - [ProcessRebuy](#angzarr_client-proto-examples-ProcessRebuy)
    - [RebuyConfig](#angzarr_client-proto-examples-RebuyConfig)
    - [RebuyDenied](#angzarr_client-proto-examples-RebuyDenied)
    - [RebuyProcessed](#angzarr_client-proto-examples-RebuyProcessed)
    - [RegistrationClosed](#angzarr_client-proto-examples-RegistrationClosed)
    - [RegistrationOpened](#angzarr_client-proto-examples-RegistrationOpened)
    - [ResumeTournament](#angzarr_client-proto-examples-ResumeTournament)
    - [StartTournament](#angzarr_client-proto-examples-StartTournament)
    - [TournamentCompleted](#angzarr_client-proto-examples-TournamentCompleted)
    - [TournamentCreated](#angzarr_client-proto-examples-TournamentCreated)
    - [TournamentEnrollmentRejected](#angzarr_client-proto-examples-TournamentEnrollmentRejected)
    - [TournamentPaused](#angzarr_client-proto-examples-TournamentPaused)
    - [TournamentPlayerEnrolled](#angzarr_client-proto-examples-TournamentPlayerEnrolled)
    - [TournamentResult](#angzarr_client-proto-examples-TournamentResult)
    - [TournamentResumed](#angzarr_client-proto-examples-TournamentResumed)
    - [TournamentStarted](#angzarr_client-proto-examples-TournamentStarted)
    - [TournamentState](#angzarr_client-proto-examples-TournamentState)
    - [TournamentState.RegisteredPlayersEntry](#angzarr_client-proto-examples-TournamentState-RegisteredPlayersEntry)
    - [UnregisterPlayer](#angzarr_client-proto-examples-UnregisterPlayer)
  
    - [TournamentStatus](#angzarr_client-proto-examples-TournamentStatus)
  
- [google/api/annotations.proto](#google_api_annotations-proto)
    - [File-level Extensions](#google_api_annotations-proto-extensions)
  
- [google/api/http.proto](#google_api_http-proto)
    - [CustomHttpPattern](#google-api-CustomHttpPattern)
    - [Http](#google-api-Http)
    - [HttpRule](#google-api-HttpRule)
  
- [Scalar Value Types](#scalar-value-types)



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/cloudevents.proto {#angzarr_client_proto_angzarr_cloudevents-proto}




### CloudEvent {#angzarr_client-proto-angzarr-CloudEvent}
region cloud_event
CloudEvent represents a single event for external consumption.

Client projectors create these by filtering/transforming internal events.
Framework fills envelope fields (id, source, time) from Cover/EventPage
if not explicitly set by the client.

The `data` field is a protobuf Any that framework converts to JSON via
prost-reflect using the descriptor pool. Clients should pack a &#34;public&#34;
proto message that omits sensitive fields.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | string |  | Event type (e.g., &#34;com.example.order.created&#34;). Default: proto type_url suffix from original event. |
| data | [google.protobuf.Any](https://protobuf.dev/reference/protobuf/google.protobuf/#any) |  | Event payload as proto Any. Framework converts to JSON for CloudEvents output. Client should filter sensitive fields before packing. |
| extensions | [CloudEvent.ExtensionsEntry](#angzarr_client-proto-angzarr-CloudEvent-ExtensionsEntry) | repeated | Custom extension attributes. Keys should follow CloudEvents naming (lowercase, no dots). Framework adds correlationid automatically if present in Cover. |
| id | string | optional | Optional overrides. Framework uses Cover/EventPage values if not set.

Default: \{domain\}:\{root_id\}:\{sequence\} |
| source | string | optional | Default: angzarr/\{domain\} |
| subject | string | optional | Default: aggregate root ID |







### CloudEvent.ExtensionsEntry {#angzarr_client-proto-angzarr-CloudEvent-ExtensionsEntry}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | string |  |  |
| value | string |  |  |







### CloudEventsResponse {#angzarr_client-proto-angzarr-CloudEventsResponse}
CloudEventsResponse is returned by client projectors in Projection.projection.

Framework detects this type by checking projection.type_url and routes
the events to configured sinks (HTTP webhook, Kafka).

Client may return 0 events (skip), 1 event (typical), or N events
(fan-out scenarios like multi-tenant notifications).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [CloudEvent](#angzarr_client-proto-angzarr-CloudEvent) | repeated |  |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/command_handler.proto {#angzarr_client_proto_angzarr_command_handler-proto}




### BusinessResponse {#angzarr_client-proto-angzarr-BusinessResponse}
Wrapper response for BusinessLogic.Handle


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Business provides compensation events |
| revocation | [RevocationResponse](#angzarr_client-proto-angzarr-RevocationResponse) |  | Business requests framework action |
| notification | [Notification](#angzarr_client-proto-angzarr-Notification) |  | Forward rejection notification upstream |







### CommandResponse {#angzarr_client-proto-angzarr-CommandResponse}
Response from entity - aggregate events &#43; sync projector results


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Events from the target aggregate |
| projections | [Projection](#angzarr_client-proto-angzarr-Projection) | repeated | Synchronous projector results |







### FactInjectionResponse {#angzarr_client-proto-angzarr-FactInjectionResponse}
region fact_injection
Response from fact injection.
Indicates whether facts were newly persisted or already existed (idempotent).

Request uses EventRequest with:
- events: EventBook containing fact events (with ExternalDeferredSequence markers in PageHeader)
- sync_mode: Controls sync processing (default: async)
- route_to_handler: Whether to invoke command handler&#39;s handle_fact (default: true)

IMPORTANT: Set PageHeader.external_deferred.external_id for idempotency. The coordinator
uses this to deduplicate fact injections - subsequent requests with the same external_id
return the original events without re-persisting.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Persisted events with real sequence numbers |
| already_processed | bool |  | True if external_id was already seen (idempotent response) |
| projections | [Projection](#angzarr_client-proto-angzarr-Projection) | repeated | Synchronous projector results (if any) |







### FactRequest {#angzarr_client-proto-angzarr-FactRequest}
Request to process fact events through aggregate business logic.
The aggregate updates its state based on external realities and returns
events to persist. The coordinator assigns real sequence numbers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| facts | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Fact events with ExternalDeferredSequence markers |
| prior_events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Prior events for state reconstruction |







### ReplayRequest {#angzarr_client-proto-angzarr-ReplayRequest}
Request to replay events and compute resulting state


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| base_snapshot | [Snapshot](#angzarr_client-proto-angzarr-Snapshot) |  | Starting state (empty = initial state) |
| events | [EventPage](#angzarr_client-proto-angzarr-EventPage) | repeated | Events to apply in order |







### ReplayResponse {#angzarr_client-proto-angzarr-ReplayResponse}
Response with computed state after replay


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [google.protobuf.Any](https://protobuf.dev/reference/protobuf/google.protobuf/#any) |  | Resulting state |







### RevocationResponse {#angzarr_client-proto-angzarr-RevocationResponse}
client logic requests framework to handle revocation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| emit_system_revocation | bool |  | Emit SagaCompensationFailed event |
| send_to_dead_letter_queue | bool |  | Send to DLQ |
| escalate | bool |  | Flag for alerting/human intervention |
| abort | bool |  | Stop saga chain, propagate error to caller |
| reason | string |  | Context/reason |







### SpeculateCommandHandlerRequest {#angzarr_client-proto-angzarr-SpeculateCommandHandlerRequest}
Request for speculative command execution against temporal state.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) |  |  |
| point_in_time | [TemporalQuery](#angzarr_client-proto-angzarr-TemporalQuery) |  |  |





 

 

 



### CommandHandlerCoordinatorService {#angzarr_client-proto-angzarr-CommandHandlerCoordinatorService}
CommandHandlerCoordinatorService: orchestrates command processing for domain aggregates

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HandleCommand | [CommandRequest](#angzarr_client-proto-angzarr-CommandRequest) | [CommandResponse](#angzarr_client-proto-angzarr-CommandResponse) | Process command with optional sync mode (default: async fire-and-forget) |
| HandleEvent | [EventRequest](#angzarr_client-proto-angzarr-EventRequest) | [FactInjectionResponse](#angzarr_client-proto-angzarr-FactInjectionResponse) | Inject fact events - external realities that cannot be rejected. Idempotent: subsequent requests with same external_id return original events. Use EventRequest.route_to_handler to control command handler invocation. |
| HandleSyncSpeculative | [SpeculateCommandHandlerRequest](#angzarr_client-proto-angzarr-SpeculateCommandHandlerRequest) | [CommandResponse](#angzarr_client-proto-angzarr-CommandResponse) | Speculative execution - execute against temporal state without persisting |
| HandleCompensation | [CommandRequest](#angzarr_client-proto-angzarr-CommandRequest) | [BusinessResponse](#angzarr_client-proto-angzarr-BusinessResponse) | Compensation flow - returns BusinessResponse for saga compensation handling. If business returns events, persists them. Caller handles revocation flags. |



### CommandHandlerService {#angzarr_client-proto-angzarr-CommandHandlerService}
CommandHandlerService: client logic that processes commands and emits events
Business logic layer that implements command handling for a domain aggregate
client logic doesn&#39;t care about sync - coordinator decides

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Handle | [ContextualCommand](#angzarr_client-proto-angzarr-ContextualCommand) | [BusinessResponse](#angzarr_client-proto-angzarr-BusinessResponse) | Process command and return business response (events or revocation request) |
| HandleFact | [FactRequest](#angzarr_client-proto-angzarr-FactRequest) | [EventBook](#angzarr_client-proto-angzarr-EventBook) | Process fact events - update aggregate state based on external realities. Optional: if unimplemented, facts are persisted as-is (pass-through). |
| Replay | [ReplayRequest](#angzarr_client-proto-angzarr-ReplayRequest) | [ReplayResponse](#angzarr_client-proto-angzarr-ReplayResponse) | Replay events to compute state (for conflict detection) Optional: only needed if aggregate supports MERGE_COMMUTATIVE |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/meta.proto {#angzarr_client_proto_angzarr_meta-proto}




### DeleteEditionEvents {#angzarr_client-proto-angzarr-DeleteEditionEvents}
Delete all events for an edition&#43;domain combination.
Main timeline (&#39;angzarr&#39; or empty edition name) cannot be deleted.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| edition | string |  | Edition name to delete from |
| domain | string |  | Domain to delete from |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/process_manager.proto {#angzarr_client_proto_angzarr_process_manager-proto}




### ProcessManagerCoordinatorRequest {#angzarr_client-proto-angzarr-ProcessManagerCoordinatorRequest}
Request for PM coordinator orchestration.
Used by CASCADE mode to invoke PM synchronously.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trigger | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Triggering events |
| sync_mode | [SyncMode](#angzarr_client-proto-angzarr-SyncMode) |  | Propagate for CASCADE recursion |
| cascade_error_mode | [CascadeErrorMode](#angzarr_client-proto-angzarr-CascadeErrorMode) |  | How to handle errors in CASCADE mode |







### ProcessManagerHandleRequest {#angzarr_client-proto-angzarr-ProcessManagerHandleRequest}
PM handle request: full context for PM decision.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| trigger | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Full state of triggering domain. |
| process_state | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Current process manager state (event-sourced). |
| destination_sequences | [ProcessManagerHandleRequest.DestinationSequencesEntry](#angzarr_client-proto-angzarr-ProcessManagerHandleRequest-DestinationSequencesEntry) | repeated | Destination sequences for command stamping (domain → next_sequence). PM should NOT rebuild destination state — use facts and let aggregates decide. |







### ProcessManagerHandleRequest.DestinationSequencesEntry {#angzarr_client-proto-angzarr-ProcessManagerHandleRequest-DestinationSequencesEntry}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | string |  |  |
| value | uint32 |  |  |







### ProcessManagerHandleResponse {#angzarr_client-proto-angzarr-ProcessManagerHandleResponse}
PM handle response: local events, then remote commands and facts.
Execution order: process_events persisted first, then commands sent, then facts injected.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| process_events | [EventBook](#angzarr_client-proto-angzarr-EventBook) | repeated | Local: Events for the process manager&#39;s own domain (non-duplicative workflow state). These are persisted via AggregateCoordinator to the PM&#39;s domain.

Audit #92: changed from singular `EventBook` to `repeated EventBook` so the merge policy (which book&#39;s cover wins, how to concatenate pages from multiple emissions) lives in the coordinator with full information, not in the client doing first-non-empty-cover-wins pre-emit. PMs that emit a single book just send a 1-element list. |
| commands | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) | repeated | Remote: Commands to issue to other aggregates. |
| facts | [EventBook](#angzarr_client-proto-angzarr-EventBook) | repeated | Remote: Facts to inject to other aggregates. Each EventBook targets a specific aggregate via its Cover. |







### SpeculatePmRequest {#angzarr_client-proto-angzarr-SpeculatePmRequest}
Request for speculative PM execution.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| request | [ProcessManagerHandleRequest](#angzarr_client-proto-angzarr-ProcessManagerHandleRequest) |  |  |





 

 

 



### ProcessManagerCoordinatorService {#angzarr_client-proto-angzarr-ProcessManagerCoordinatorService}
ProcessManagerCoordinatorService: orchestrates PM execution

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Handle | [ProcessManagerCoordinatorRequest](#angzarr_client-proto-angzarr-ProcessManagerCoordinatorRequest) | [ProcessManagerHandleResponse](#angzarr_client-proto-angzarr-ProcessManagerHandleResponse) | Full orchestration with sync mode. Used by CASCADE mode to call PMs synchronously. |
| HandleSpeculative | [SpeculatePmRequest](#angzarr_client-proto-angzarr-SpeculatePmRequest) | [ProcessManagerHandleResponse](#angzarr_client-proto-angzarr-ProcessManagerHandleResponse) | Speculative execution - returns commands and events without persisting |



### ProcessManagerService {#angzarr_client-proto-angzarr-ProcessManagerService}
ProcessManagerService: stateful coordinator for long-running workflows across multiple aggregates.

WARNING: Only use when saga &#43; queries is insufficient. Consider:
- Can a simple saga &#43; destination queries solve this?
- Is the &#34;state&#34; you want to track already derivable from existing aggregates?
- Are you adding Process Manager because the workflow is genuinely complex?

Process Manager is warranted when:
- Workflow state is NOT derivable from aggregates (PM owns unique state)
- You need to query workflow status independently (&#34;show all pending fulfillments&#34;)
- Timeout/scheduling logic is complex enough to merit its own aggregate
- You must react to events from MULTIPLE domains (saga recommends single domain)

Process Manager IS an aggregate with its own domain, events, and state.
It reuses all aggregate infrastructure (EventStore, SnapshotStore, AggregateCoordinator).

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Handle | [ProcessManagerHandleRequest](#angzarr_client-proto-angzarr-ProcessManagerHandleRequest) | [ProcessManagerHandleResponse](#angzarr_client-proto-angzarr-ProcessManagerHandleResponse) | Handle with trigger &#43; process state. Returns commands for other aggregates and events for the PM&#39;s own domain.

PMs do not rebuild destination aggregate state — they translate events into commands/facts and rely on destination_sequences for command stamping. See process manager design philosophy. |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/projector.proto {#angzarr_client_proto_angzarr_projector-proto}




### SpeculateProjectorRequest {#angzarr_client-proto-angzarr-SpeculateProjectorRequest}
Request for speculative projector execution.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  |  |





 

 

 



### ProjectorCoordinatorService {#angzarr_client-proto-angzarr-ProjectorCoordinatorService}
ProjectorCoordinatorService: orchestrates projection processing

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| HandleSync | [EventRequest](#angzarr_client-proto-angzarr-EventRequest) | [Projection](#angzarr_client-proto-angzarr-Projection) | Sync processing - returns projection based on sync_mode |
| Handle | [EventBook](#angzarr_client-proto-angzarr-EventBook) | [.google.protobuf.Empty](https://protobuf.dev/reference/protobuf/google.protobuf/#empty) | Async processing - fire and forget |
| HandleSpeculative | [SpeculateProjectorRequest](#angzarr_client-proto-angzarr-SpeculateProjectorRequest) | [Projection](#angzarr_client-proto-angzarr-Projection) | Speculative processing - returns projection without side effects |



### ProjectorService {#angzarr_client-proto-angzarr-ProjectorService}
ProjectorService: client logic that projects events to read models
client logic doesn&#39;t care about sync - coordinator decides

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Handle | [EventBook](#angzarr_client-proto-angzarr-EventBook) | [Projection](#angzarr_client-proto-angzarr-Projection) | Async projection - projector should persist and return |
| HandleSpeculative | [EventBook](#angzarr_client-proto-angzarr-EventBook) | [Projection](#angzarr_client-proto-angzarr-Projection) | Speculative processing - projector must avoid external side effects |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/query.proto {#angzarr_client_proto_angzarr_query-proto}


 

 

 



### EventQueryService {#angzarr_client-proto-angzarr-EventQueryService}
EventQueryService: query interface for retrieving events

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetEventBook | [Query](#angzarr_client-proto-angzarr-Query) | [EventBook](#angzarr_client-proto-angzarr-EventBook) | Get a single EventBook (unary) - use for explicit queries with gRPC tooling |
| GetEvents | [Query](#angzarr_client-proto-angzarr-Query) | [EventBook](#angzarr_client-proto-angzarr-EventBook) stream | Stream EventBooks matching query - use for bulk retrieval (SSE) |
| Synchronize | [Query](#angzarr_client-proto-angzarr-Query) stream | [EventBook](#angzarr_client-proto-angzarr-EventBook) stream | Bidirectional sync - not exposed via REST (use gRPC directly) |
| GetAggregateRoots | [.google.protobuf.Empty](https://protobuf.dev/reference/protobuf/google.protobuf/#empty) | [AggregateRoot](#angzarr_client-proto-angzarr-AggregateRoot) stream | List all aggregate roots (SSE) |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/saga.proto {#angzarr_client_proto_angzarr_saga-proto}




### SagaCompensationFailed {#angzarr_client-proto-angzarr-SagaCompensationFailed}
System event when compensation fails/requested


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| triggering_aggregate | [Cover](#angzarr_client-proto-angzarr-Cover) |  |  |
| triggering_event_sequence | uint32 |  |  |
| rejection_reason | string |  |  |
| compensation_failure_reason | string |  |  |
| rejected_command | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) |  |  |
| occurred_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### SagaHandleRequest {#angzarr_client-proto-angzarr-SagaHandleRequest}
Request for saga execution.
Sagas are pure translators: source events → commands.
Destination sequences provided for command stamping.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Source events that triggered the saga |
| sync_mode | [SyncMode](#angzarr_client-proto-angzarr-SyncMode) |  | Propagate for CASCADE recursion |
| cascade_error_mode | [CascadeErrorMode](#angzarr_client-proto-angzarr-CascadeErrorMode) |  | How to handle errors in CASCADE mode |
| destination_sequences | [SagaHandleRequest.DestinationSequencesEntry](#angzarr_client-proto-angzarr-SagaHandleRequest-DestinationSequencesEntry) | repeated | domain → next_sequence for command stamping |







### SagaHandleRequest.DestinationSequencesEntry {#angzarr_client-proto-angzarr-SagaHandleRequest-DestinationSequencesEntry}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | string |  |  |
| value | uint32 |  |  |







### SagaResponse {#angzarr_client-proto-angzarr-SagaResponse}
Response from saga - commands for other aggregates


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commands | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) | repeated | Commands to execute on other aggregates (with angzarr_deferred) |
| events | [EventBook](#angzarr_client-proto-angzarr-EventBook) | repeated | Events (facts) to inject directly |







### SpeculateSagaRequest {#angzarr_client-proto-angzarr-SpeculateSagaRequest}
Request for speculative saga execution.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| request | [SagaHandleRequest](#angzarr_client-proto-angzarr-SagaHandleRequest) |  |  |





 

 

 



### SagaCoordinatorService {#angzarr_client-proto-angzarr-SagaCoordinatorService}
SagaCoordinatorService: orchestrates saga execution.
Framework handles sequence stamping and delivery retry.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Execute | [SagaHandleRequest](#angzarr_client-proto-angzarr-SagaHandleRequest) | [SagaResponse](#angzarr_client-proto-angzarr-SagaResponse) | Execute saga: translate source events → commands, deliver to targets |
| ExecuteSpeculative | [SpeculateSagaRequest](#angzarr_client-proto-angzarr-SpeculateSagaRequest) | [SagaResponse](#angzarr_client-proto-angzarr-SagaResponse) | Speculative execution - returns commands without side effects |



### SagaService {#angzarr_client-proto-angzarr-SagaService}
SagaService: stateless translation from source events to commands.
Sagas receive only source events — framework handles sequence stamping and delivery.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Handle | [SagaHandleRequest](#angzarr_client-proto-angzarr-SagaHandleRequest) | [SagaResponse](#angzarr_client-proto-angzarr-SagaResponse) | Translate source events into commands for target domains. Commands use angzarr_deferred — framework stamps explicit sequences on delivery. |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/stream.proto {#angzarr_client_proto_angzarr_stream-proto}


 

 

 



### EventStreamService {#angzarr_client-proto-angzarr-EventStreamService}
region event_stream_service
EventStreamService: streams events to registered subscribers

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Subscribe | [EventStreamFilter](#angzarr_client-proto-angzarr-EventStreamFilter) | [EventBook](#angzarr_client-proto-angzarr-EventBook) stream | Subscribe to events matching correlation ID (required) Returns INVALID_ARGUMENT if correlation_id is empty REST: Server-Sent Events (SSE) stream |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/types.proto {#angzarr_client_proto_angzarr_types-proto}




### AggregateRoot {#angzarr_client-proto-angzarr-AggregateRoot}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| domain | string |  |  |
| root | [UUID](#angzarr_client-proto-angzarr-UUID) |  |  |







### AngzarrDeadLetter {#angzarr_client-proto-angzarr-AngzarrDeadLetter}
region dead_letter
Dead letter queue entry for failed messages requiring manual intervention.
Per-domain topics: angzarr.dlq.\{domain\}


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cover | [Cover](#angzarr_client-proto-angzarr-Cover) |  | Routing: domain, root, correlation_id |
| rejected_command | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) |  | Command that failed |
| rejected_events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Events that failed (saga/projector failures) |
| rejection_reason | string |  | Human-readable reason |
| sequence_mismatch | [SequenceMismatchDetails](#angzarr_client-proto-angzarr-SequenceMismatchDetails) |  | Sequence conflict details |
| event_processing_failed | [EventProcessingFailedDetails](#angzarr_client-proto-angzarr-EventProcessingFailedDetails) |  | Handler failure details |
| payload_retrieval_failed | [PayloadRetrievalFailedDetails](#angzarr_client-proto-angzarr-PayloadRetrievalFailedDetails) |  | Payload store failure details |
| occurred_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| metadata | [AngzarrDeadLetter.MetadataEntry](#angzarr_client-proto-angzarr-AngzarrDeadLetter-MetadataEntry) | repeated | Additional context |
| source_component | string |  | Which component sent to DLQ |
| source_component_type | string |  | &#34;aggregate&#34; | &#34;saga&#34; | &#34;projector&#34; | &#34;process_manager&#34; |







### AngzarrDeadLetter.MetadataEntry {#angzarr_client-proto-angzarr-AngzarrDeadLetter-MetadataEntry}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | string |  |  |
| value | string |  |  |







### AngzarrDeferredSequence {#angzarr_client-proto-angzarr-AngzarrDeferredSequence}
For saga-produced commands and facts.
Framework stamps sequence on delivery; idempotency derived from source info.
Rejections route back to source aggregate.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| source | [Cover](#angzarr_client-proto-angzarr-Cover) |  | Full source aggregate (domain &#43; root &#43; edition) - rejection routes here |
| source_seq | uint32 |  | Sequence of the triggering event |







### CascadeCommit {#angzarr_client-proto-angzarr-CascadeCommit}
region cascade_commit
PM/coordinator emits to commit all uncommitted events for a cascade.
Framework distributes Confirmation to all participants.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cascade_id | string |  |  |







### CascadeConflictDetail {#angzarr_client-proto-angzarr-CascadeConflictDetail}
region cascade_conflict_detail
Error detail for conflict rejection when overlapping fields are locked.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cascade_ids | string | repeated | Cascades holding locks |
| overlapping_fields | string | repeated | Fields that conflict |







### CascadeRollback {#angzarr_client-proto-angzarr-CascadeRollback}
region cascade_rollback
PM/coordinator emits to rollback all uncommitted events for a cascade.
Framework distributes Revocation to all participants.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cascade_id | string |  |  |
| reason | string |  |  |







### CommandBook {#angzarr_client-proto-angzarr-CommandBook}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cover | [Cover](#angzarr_client-proto-angzarr-Cover) |  |  |
| pages | [CommandPage](#angzarr_client-proto-angzarr-CommandPage) | repeated | Field 3 removed: correlation_id moved to Cover Field 4 removed: saga_origin moved to PageHeader.angzarr_deferred Field 5 removed: &#39;fact&#39; was unused |







### CommandPage {#angzarr_client-proto-angzarr-CommandPage}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [PageHeader](#angzarr_client-proto-angzarr-PageHeader) |  | Sequence type and provenance |
| merge_strategy | [MergeStrategy](#angzarr_client-proto-angzarr-MergeStrategy) |  |  |
| command | [google.protobuf.Any](https://protobuf.dev/reference/protobuf/google.protobuf/#any) |  |  |
| external | [PayloadReference](#angzarr_client-proto-angzarr-PayloadReference) |  | Claim check: payload stored externally |







### CommandRequest {#angzarr_client-proto-angzarr-CommandRequest}
Request wrapper for command operations.
Adds execution metadata (sync_mode, cascade_error_mode) to CommandBook.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) |  |  |
| sync_mode | [SyncMode](#angzarr_client-proto-angzarr-SyncMode) |  |  |
| cascade_error_mode | [CascadeErrorMode](#angzarr_client-proto-angzarr-CascadeErrorMode) |  | How to handle saga/PM errors in CASCADE mode |
| cascade_id | string | optional | If set, enables 2PC: events written with committed=false |







### Compensate {#angzarr_client-proto-angzarr-Compensate}
region compensate
Routes to client compensation handler - original events remain visible.
Unlike Revocation, this triggers client-implemented inverse logic.
The Compensate marker itself is filtered to NoOp; original events stay visible.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target | [Cover](#angzarr_client-proto-angzarr-Cover) |  | Domain &#43; root of aggregate |
| sequences | uint32 | repeated | Sequences to compensate |
| reason | string |  | Why compensation is needed |







### ComponentDescriptor {#angzarr_client-proto-angzarr-ComponentDescriptor}
Component self-description.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | string |  |  |
| component_type | string |  |  |
| inputs | [Target](#angzarr_client-proto-angzarr-Target) | repeated | Domains I subscribe to (event types I consume) |







### Confirmation {#angzarr_client-proto-angzarr-Confirmation}
region confirmation
Confirms pending events - makes them visible to business logic.
Written by framework when all sagas in a cascade succeed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target | [Cover](#angzarr_client-proto-angzarr-Cover) |  | Domain &#43; root of aggregate being confirmed |
| sequences | uint32 | repeated | Explicit list of sequences being confirmed |
| cascade_id | string |  | Set for cascade commits, empty for general confirmation |







### ContextualCommand {#angzarr_client-proto-angzarr-ContextualCommand}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  | Passed from aggregate coordinator to aggregate, consists of everything needed to execute/evaluate the command |
| command | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) |  |  |







### ContextualCommandRequest {#angzarr_client-proto-angzarr-ContextualCommandRequest}
Request wrapper for contextual command operations (internal use).
Adds execution metadata (sync_mode) to ContextualCommand.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [ContextualCommand](#angzarr_client-proto-angzarr-ContextualCommand) |  |  |
| sync_mode | [SyncMode](#angzarr_client-proto-angzarr-SyncMode) |  |  |







### Cover {#angzarr_client-proto-angzarr-Cover}
region cover


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| domain | string |  |  |
| root | [UUID](#angzarr_client-proto-angzarr-UUID) |  |  |
| correlation_id | string |  | Workflow correlation - flows through all commands/events |
| edition | [Edition](#angzarr_client-proto-angzarr-Edition) |  | Edition for diverged timelines; empty name = main timeline |







### DomainDivergence {#angzarr_client-proto-angzarr-DomainDivergence}
Explicit divergence point for a specific domain.
Used when creating historical branches or coordinating saga writes across domains.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| domain | string |  | Domain name |
| sequence | uint32 |  | Divergence sequence number |







### Edition {#angzarr_client-proto-angzarr-Edition}
region edition
Edition identifier with optional explicit divergence points.

Two modes:
- Implicit (divergences empty): Divergence derived from first edition event&#39;s sequence
- Explicit (divergences populated): Per-domain divergence points for historical branching,
 saga coordination, or speculative execution


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | string |  | Edition name, e.g., &#34;v2&#34;; empty = main timeline |
| divergences | [DomainDivergence](#angzarr_client-proto-angzarr-DomainDivergence) | repeated | Optional: explicit per-domain divergence points |







### EventBook {#angzarr_client-proto-angzarr-EventBook}
region event_book


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cover | [Cover](#angzarr_client-proto-angzarr-Cover) |  |  |
| snapshot | [Snapshot](#angzarr_client-proto-angzarr-Snapshot) |  | Snapshot state; sequence computed by framework on persist |
| pages | [EventPage](#angzarr_client-proto-angzarr-EventPage) | repeated |  |
| next_sequence | uint32 |  | Field 4 removed: correlation_id moved to Cover Field 5 removed: snapshot_state unified into snapshot field

Computed on load, never stored: (last page seq OR snapshot seq if no pages) &#43; 1 |







### EventPage {#angzarr_client-proto-angzarr-EventPage}
region event_page


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [PageHeader](#angzarr_client-proto-angzarr-PageHeader) |  | Sequence type and provenance |
| created_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| event | [google.protobuf.Any](https://protobuf.dev/reference/protobuf/google.protobuf/#any) |  |  |
| external | [PayloadReference](#angzarr_client-proto-angzarr-PayloadReference) |  | Claim check: payload stored externally |
| no_commit | bool |  | Two-phase commit support (Phase 1: 2PC Storage Model)

true = pending 2PC (cascade), needs Confirmation; false (default) = immediately committed |
| cascade_id | string | optional | Groups related pending events for atomic commit/rollback (null if not in cascade) |







### EventProcessingFailedDetails {#angzarr_client-proto-angzarr-EventProcessingFailedDetails}
Event processing failure details for DLQ entries.
Contains information about why a saga/projector failed to process events.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | string |  | Error message from the handler |
| retry_count | uint32 |  | Number of retry attempts before DLQ routing |
| is_transient | bool |  | Whether the failure is considered transient |
| stack_trace | [sererr.v1.CapturedError](#sererr-v1-CapturedError) | repeated | Flat array of captured errors representing the cause chain (most-causal-first; the originating caught error is the LAST element). Matches Sentry&#39;s `exception.values` shape; linkage via each entry&#39;s `mechanism.exception_id` / `mechanism.parent_id`. Empty when no capture was attempted. See sererr.fyi/spec/proto for the schema rationale. |







### EventRequest {#angzarr_client-proto-angzarr-EventRequest}
Request wrapper for event operations (fact injection).
Adds execution metadata (sync_mode, route_to_handler) to EventBook.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [EventBook](#angzarr_client-proto-angzarr-EventBook) |  |  |
| sync_mode | [SyncMode](#angzarr_client-proto-angzarr-SyncMode) |  |  |
| route_to_handler | bool |  | For fact injection: when true (default), invokes command handler&#39;s handle_fact for validation/error checking before persistence. Facts cannot be rejected, but the handler can validate data integrity and log warnings. When false, facts are persisted directly without handler involvement. |







### EventStreamFilter {#angzarr_client-proto-angzarr-EventStreamFilter}
region event_stream_filter
Subscription filter for event streaming


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| correlation_id | string |  |  |







### ExternalDeferredSequence {#angzarr_client-proto-angzarr-ExternalDeferredSequence}
For facts from external systems (webhooks, integrations).
Framework stamps sequence on delivery; idempotency via external_id.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| external_id | string |  | Idempotency key from external system (e.g., &#34;pi_1234&#34; from Stripe) |
| description | string |  | Human-readable origin (e.g., &#34;Stripe webhook&#34;) |







### GetDescriptorRequest {#angzarr_client-proto-angzarr-GetDescriptorRequest}
Request for GetDescriptor RPC.







### NoOp {#angzarr_client-proto-angzarr-NoOp}
region noop
Placeholder returned by coordinator for uncommitted/framework events.
Never persisted - only returned at read time for filtered events.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| original_sequence | uint32 |  | Sequence of the filtered event |
| cascade_id | string |  | Which cascade it belonged to (if pending) |
| reason | string |  | &#34;uncommitted&#34;, &#34;revoked&#34;, &#34;framework_event&#34; |







### Notification {#angzarr_client-proto-angzarr-Notification}
region notification
Base notification message for transient system signals.
Contains routing info via Cover but no persistence semantics.
Type discrimination via payload.type_url (standard Any behavior).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cover | [Cover](#angzarr_client-proto-angzarr-Cover) |  | Routing: domain, root, correlation_id |
| payload | [google.protobuf.Any](https://protobuf.dev/reference/protobuf/google.protobuf/#any) |  | Type-specific content (RejectionNotification, etc.) |
| sent_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  | When notification was created |







### PageHeader {#angzarr_client-proto-angzarr-PageHeader}
region page_header
Shared header for CommandPage and EventPage.
Encodes sequence type and provenance for framework processing.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sequence | uint32 |  | Explicit sequence (aggregate handlers, legacy) |
| external_deferred | [ExternalDeferredSequence](#angzarr_client-proto-angzarr-ExternalDeferredSequence) |  | External fact (Stripe, FedEx, etc.) |
| angzarr_deferred | [AngzarrDeferredSequence](#angzarr_client-proto-angzarr-AngzarrDeferredSequence) |  | Saga-produced command/fact |
| sync_mode | [SyncMode](#angzarr_client-proto-angzarr-SyncMode) | optional | Per-command override of the enclosing CommandRequest&#39;s sync_mode. Only meaningful on CommandPage headers; ignored on EventPage headers. PMs emit a `repeated CommandBook` and cannot reach the request wrapper themselves; setting sync_mode here lets a PM tag a single emitted command (e.g. SYNC_MODE_DECISION when its accept/reject must surface synchronously) while the surrounding flow stays whatever the original caller asked for. When unset (the common case) coordinators inherit CommandRequest.sync_mode unchanged. |







### PayloadReference {#angzarr_client-proto-angzarr-PayloadReference}
Reference to externally stored payload (claim check pattern).
Used when event/command payloads exceed message bus size limits.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_type | [PayloadStorageType](#angzarr_client-proto-angzarr-PayloadStorageType) |  |  |
| uri | string |  | Location URI: - file:///var/angzarr/payloads/\{hash\}.bin - gs://bucket/prefix/\{hash\}.bin - s3://bucket/prefix/\{hash\}.bin |
| content_hash | bytes |  | Content hash for integrity verification and deduplication (SHA-256) |
| original_size | uint64 |  | Original serialized payload size in bytes |
| stored_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  | Timestamp when payload was stored (for TTL cleanup) |







### PayloadRetrievalFailedDetails {#angzarr_client-proto-angzarr-PayloadRetrievalFailedDetails}
Payload retrieval failure details for DLQ entries.
Contains information about why an externally stored payload couldn&#39;t be retrieved.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_type | [PayloadStorageType](#angzarr_client-proto-angzarr-PayloadStorageType) |  | Storage backend type |
| uri | string |  | URI of the payload that couldn&#39;t be retrieved |
| content_hash | bytes |  | Content hash for identification |
| original_size | uint64 |  | Original payload size in bytes |
| error | string |  | Error message from the retrieval attempt |







### Projection {#angzarr_client-proto-angzarr-Projection}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cover | [Cover](#angzarr_client-proto-angzarr-Cover) |  |  |
| projector | string |  |  |
| sequence | uint32 |  |  |
| projection | [google.protobuf.Any](https://protobuf.dev/reference/protobuf/google.protobuf/#any) |  |  |







### Query {#angzarr_client-proto-angzarr-Query}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cover | [Cover](#angzarr_client-proto-angzarr-Cover) |  | Cover identifies the aggregate: domain &#43; (root | correlation_id | both) Query by root: Cover \{ domain, root \} Query by correlation: Cover \{ domain, correlation_id \} |
| range | [SequenceRange](#angzarr_client-proto-angzarr-SequenceRange) |  |  |
| sequences | [SequenceSet](#angzarr_client-proto-angzarr-SequenceSet) |  |  |
| temporal | [TemporalQuery](#angzarr_client-proto-angzarr-TemporalQuery) |  |  |







### RejectionNotification {#angzarr_client-proto-angzarr-RejectionNotification}
region rejection_notification
Notification payload for command rejection scenarios.
Embedded in Notification.payload when a saga/PM command is rejected.

Source info for compensation is in rejected_command.pages[].header.angzarr_deferred:
- source.domain, source.root, source.edition → where to route rejection
- source_seq → which event triggered the command


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rejected_command | [CommandBook](#angzarr_client-proto-angzarr-CommandBook) |  | The command that was rejected (full context) |
| rejection_reason | string |  | Why: &#34;insufficient_funds&#34;, &#34;out_of_stock&#34;, etc. |







### Revocation {#angzarr_client-proto-angzarr-Revocation}
region revocation
Revokes events - marks them as NoOp at read time.
Written by framework on cascade failure, timeout, or explicit revocation API.
Original events become invisible to business logic.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target | [Cover](#angzarr_client-proto-angzarr-Cover) |  | Domain &#43; root of aggregate being revoked |
| sequences | uint32 | repeated | Sequences being revoked |
| cascade_id | string |  | Set for cascade rollbacks, empty for general revocation |
| reason | string |  | &#34;saga_failed&#34;, &#34;timeout&#34;, &#34;compensation&#34;, etc. |







### SequenceMismatchDetails {#angzarr_client-proto-angzarr-SequenceMismatchDetails}
region dlq_details
Sequence mismatch details for DLQ entries.
Contains expected vs actual sequence for debugging and replay.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| expected_sequence | uint32 |  | What the command expected |
| actual_sequence | uint32 |  | What the aggregate was at |
| merge_strategy | [MergeStrategy](#angzarr_client-proto-angzarr-MergeStrategy) |  | Strategy that triggered DLQ routing |







### SequenceRange {#angzarr_client-proto-angzarr-SequenceRange}
Query types


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lower | uint32 |  |  |
| upper | uint32 | optional | If not set, query to latest |







### SequenceSet {#angzarr_client-proto-angzarr-SequenceSet}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| values | uint32 | repeated |  |







### Snapshot {#angzarr_client-proto-angzarr-Snapshot}
region aggregate_snapshot
Snapshot of aggregate state at a given sequence number.
State must be a protobuf Message to serialize into Any.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sequence | uint32 |  |  |
| state | [google.protobuf.Any](https://protobuf.dev/reference/protobuf/google.protobuf/#any) |  |  |
| retention | [SnapshotRetention](#angzarr_client-proto-angzarr-SnapshotRetention) |  | Controls cleanup behavior |







### Target {#angzarr_client-proto-angzarr-Target}
Describes what a component subscribes to.
Topology edges derived from inputs: if A subscribes to domain X, edge X→A exists.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| domain | string |  |  |







### TemporalQuery {#angzarr_client-proto-angzarr-TemporalQuery}
Temporal query: retrieve aggregate state at a point in history.
Replays events from sequence 0 (no snapshots) to the specified point.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| as_of_time | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  | Events with created_at &lt;= this |
| as_of_sequence | uint32 |  | Events with sequence &lt;= this |







### UUID {#angzarr_client-proto-angzarr-UUID}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | bytes |  |  |





 



### CascadeErrorMode {#angzarr_client-proto-angzarr-CascadeErrorMode}
region cascade_error_mode
Controls how CASCADE mode handles errors from sagas/PMs.
Only relevant when sync_mode = CASCADE.

| Name | Number | Description |
| ---- | ------ | ----------- |
| CASCADE_ERROR_FAIL_FAST | 0 | Stop on first error, fail request (default) |
| CASCADE_ERROR_CONTINUE | 1 | Continue through all, return successes &#43; errors |
| CASCADE_ERROR_COMPENSATE | 2 | On first error, compensate executed commands, fail request |
| CASCADE_ERROR_DEAD_LETTER | 3 | On error, send to DLQ and continue with remaining |




### MergeStrategy {#angzarr_client-proto-angzarr-MergeStrategy}
region merge_strategy
Controls how concurrent commands to the same aggregate are handled

| Name | Number | Description |
| ---- | ------ | ----------- |
| MERGE_COMMUTATIVE | 0 | Default: allow if state field mutations don&#39;t overlap |
| MERGE_STRICT | 1 | Reject if sequence mismatch (optimistic concurrency) |
| MERGE_AGGREGATE_HANDLES | 2 | Aggregate handles its own concurrency |
| MERGE_MANUAL | 3 | Send to DLQ for manual review on mismatch |




### PayloadStorageType {#angzarr_client-proto-angzarr-PayloadStorageType}
region payload_reference
Storage backend type for externally stored payloads (claim check pattern).

| Name | Number | Description |
| ---- | ------ | ----------- |
| PAYLOAD_STORAGE_TYPE_UNSPECIFIED | 0 |  |
| PAYLOAD_STORAGE_TYPE_FILESYSTEM | 1 |  |
| PAYLOAD_STORAGE_TYPE_GCS | 2 |  |
| PAYLOAD_STORAGE_TYPE_S3 | 3 |  |




### SnapshotRetention {#angzarr_client-proto-angzarr-SnapshotRetention}
region snapshot_retention
Controls snapshot retention during cleanup

| Name | Number | Description |
| ---- | ------ | ----------- |
| RETENTION_DEFAULT | 0 | Persist every 16 events, treated as TRANSIENT otherwise |
| RETENTION_PERSIST | 1 | Keep indefinitely (business milestone) |
| RETENTION_TRANSIENT | 2 | Delete when newer snapshot written |




### SyncMode {#angzarr_client-proto-angzarr-SyncMode}
region sync_mode
Controls synchronous processing behavior.

Impact ordering (lowest to highest):
  ASYNC  &lt; DECISION &lt; SIMPLE &lt; CASCADE

Primary caller of DECISION is process managers coordinating cross-aggregate
flows: the PM issues a command to the target aggregate and needs to know
*only* whether the aggregate accepted or rejected, without paying the cost
of projector propagation or saga fan-out. The aggregate&#39;s accept path
(events persisted &#43; returned) and reject path (CommandRejectedError) are
both observable; everything downstream runs asynchronously.

| Name | Number | Description |
| ---- | ------ | ----------- |
| SYNC_MODE_ASYNC | 0 | Async: fire and forget (default) |
| SYNC_MODE_SIMPLE | 1 | Sync projectors only, no saga cascade |
| SYNC_MODE_CASCADE | 2 | Full sync: projectors &#43; saga cascade (expensive) |
| SYNC_MODE_DECISION | 3 | Sync aggregate accept/reject only; projectors &#43; sagas run async |
| SYNC_MODE_ISOLATED | 4 | Sync accept/reject &#43; persist; NO downstream (sync OR async). Replay / migration / recovery writes that must not trigger reactions. Distinct from DECISION (which still publishes to bus for async downstream). |


 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/angzarr/upcaster.proto {#angzarr_client_proto_angzarr_upcaster-proto}




### UpcastRequest {#angzarr_client-proto-angzarr-UpcastRequest}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| domain | string |  |  |
| events | [EventPage](#angzarr_client-proto-angzarr-EventPage) | repeated |  |







### UpcastResponse {#angzarr_client-proto-angzarr-UpcastResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [EventPage](#angzarr_client-proto-angzarr-EventPage) | repeated |  |





 

 

 



### UpcasterService {#angzarr_client-proto-angzarr-UpcasterService}
UpcasterService: transforms old event versions to current versions
Implemented by the client alongside AggregateService on the same gRPC server.
Optionally can be deployed as a separate binary for testing or complex migrations.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Upcast | [UpcastRequest](#angzarr_client-proto-angzarr-UpcastRequest) | [UpcastResponse](#angzarr_client-proto-angzarr-UpcastResponse) | Transform events to current version Returns events in same order, transformed where applicable |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/ai_sidecar.proto {#angzarr_client_proto_examples_ai_sidecar-proto}




### ActionContext {#angzarr_client-proto-examples-ActionContext}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | string |  |  |
| player_root | bytes |  |  |
| hand_id | bytes |  |  |
| snapshot | [ActionRequest](#angzarr_client-proto-examples-ActionRequest) |  |  |
| events | [HandEvent](#angzarr_client-proto-examples-HandEvent) | repeated |  |







### ActionHistory {#angzarr_client-proto-examples-ActionHistory}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| action | [ActionType](#angzarr_client-proto-examples-ActionType) |  |  |
| amount | int64 |  |  |
| phase | [BettingPhase](#angzarr_client-proto-examples-BettingPhase) |  |  |







### ActionRequest {#angzarr_client-proto-examples-ActionRequest}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | string |  | Which model variant to use. Also used as session_id when GetAction is invoked outside an explicit session (backwards-compatible behavior). |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| phase | [BettingPhase](#angzarr_client-proto-examples-BettingPhase) |  |  |
| hole_cards | [Card](#angzarr_client-proto-examples-Card) | repeated | Cards |
| community_cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| pot_size | int64 |  | Betting context (chip amounts are int64; never uint64 for JS compat). |
| stack_size | int64 |  |  |
| amount_to_call | int64 |  |  |
| min_raise | int64 |  |  |
| max_raise | int64 |  |  |
| position | int32 |  | Position info

0 = button, increasing = earlier |
| players_remaining | int32 |  |  |
| players_to_act | int32 |  |  |
| action_history | [ActionHistory](#angzarr_client-proto-examples-ActionHistory) | repeated | Historical context (for recurrent models). |
| opponents | [OpponentStats](#angzarr_client-proto-examples-OpponentStats) | repeated | Opponent modeling snapshot (optional). |







### ActionResponse {#angzarr_client-proto-examples-ActionResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| recommended_action | [ActionType](#angzarr_client-proto-examples-ActionType) |  |  |
| amount | int64 |  | For bet/raise |
| fold_probability | float |  | Confidence scores for each action (for analysis). |
| check_call_probability | float |  |  |
| bet_raise_probability | float |  |  |
| model_version | string |  | Model metadata |
| inference_time_ms | int64 |  |  |







### BatchActionRequest {#angzarr_client-proto-examples-BatchActionRequest}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| requests | [ActionRequest](#angzarr_client-proto-examples-ActionRequest) | repeated |  |







### BatchActionResponse {#angzarr_client-proto-examples-BatchActionResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| responses | [ActionResponse](#angzarr_client-proto-examples-ActionResponse) | repeated |  |







### EndSessionRequest {#angzarr_client-proto-examples-EndSessionRequest}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | string |  |  |
| persist_stats | bool |  | If true, merge accumulated opponent stats into persistent profiles. |







### EndSessionResponse {#angzarr_client-proto-examples-EndSessionResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | bool |  |  |
| hands_played | int32 |  |  |
| total_result | int64 |  |  |







### Experience {#angzarr_client-proto-examples-Experience}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| context | [ActionContext](#angzarr_client-proto-examples-ActionContext) |  |  |
| action_taken | [ActionType](#angzarr_client-proto-examples-ActionType) |  |  |
| amount | int64 |  |  |
| log_prob | float |  |  |
| value_estimate | float |  |  |
| reward | float |  |  |
| terminal | bool |  |  |







### HandEvent {#angzarr_client-proto-examples-HandEvent}
HandEvent uses a oneof over the canonical hand-domain event types defined
in hand.proto. Reusing those shapes keeps one vocabulary across aggregates,
sidecars, and language clients — the AI does not redefine what a &#34;deal&#34;
or &#34;showdown&#34; means; it subscribes to the same events the hand aggregate
emits. Readers switch on WhichOneof(&#39;event&#39;) in Python, EventCase in C#,
etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cards_dealt | [CardsDealt](#angzarr_client-proto-examples-CardsDealt) |  |  |
| community_dealt | [CommunityCardsDealt](#angzarr_client-proto-examples-CommunityCardsDealt) |  |  |
| action_taken | [ActionTaken](#angzarr_client-proto-examples-ActionTaken) |  |  |
| showdown | [ShowdownStarted](#angzarr_client-proto-examples-ShowdownStarted) |  |  |
| pot_awarded | [PotAwarded](#angzarr_client-proto-examples-PotAwarded) |  |  |







### HealthRequest {#angzarr_client-proto-examples-HealthRequest}








### HealthResponse {#angzarr_client-proto-examples-HealthResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| healthy | bool |  |  |
| model_id | string |  |  |
| model_version | string |  |  |
| uptime_seconds | int64 |  |  |
| requests_served | int64 |  |  |







### OpponentProfile {#angzarr_client-proto-examples-OpponentProfile}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| total_hands | int32 |  |  |
| vpip | float |  |  |
| pfr | float |  |  |
| af | float |  |  |
| wtsd | float |  |  |
| w_sd | float |  |  |
| avg_decision_time_ms | float |  |  |
| hands_since_update | int32 |  |  |







### OpponentQuery {#angzarr_client-proto-examples-OpponentQuery}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_roots | bytes | repeated |  |







### OpponentStats {#angzarr_client-proto-examples-OpponentStats}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| position | int32 |  |  |
| stack | int64 |  |  |
| vpip | float |  | Voluntarily put in pot % |
| pfr | float |  | Pre-flop raise % |
| aggression | float |  | Bet/raise frequency |
| hands_played | int32 |  |  |







### OpponentStatsResponse {#angzarr_client-proto-examples-OpponentStatsResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| profiles | [OpponentProfile](#angzarr_client-proto-examples-OpponentProfile) | repeated |  |







### RecordResponse {#angzarr_client-proto-examples-RecordResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | bool |  |  |
| message | string |  |  |
| experience_id | int64 |  |  |







### ReloadModelRequest {#angzarr_client-proto-examples-ReloadModelRequest}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | string |  | Target model slot; empty means &#34;default&#34; slot. |
| model_path | string |  | Filesystem path to the checkpoint the sidecar should load. |







### ReloadModelResponse {#angzarr_client-proto-examples-ReloadModelResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | bool |  |  |
| message | string |  |  |
| model_version | string |  |  |







### StartSessionRequest {#angzarr_client-proto-examples-StartSessionRequest}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| session_id | string |  |  |
| ai_player_root | bytes |  |  |
| model_id | string |  |  |







### StartSessionResponse {#angzarr_client-proto-examples-StartSessionResponse}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | bool |  |  |
| session_id | string |  |  |
| model_version | string |  |  |





 

 

 



### AiSidecar {#angzarr_client-proto-examples-AiSidecar}


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetAction | [ActionRequest](#angzarr_client-proto-examples-ActionRequest) | [ActionResponse](#angzarr_client-proto-examples-ActionResponse) | Get recommended action from the AI model for a single decision point. |
| Health | [HealthRequest](#angzarr_client-proto-examples-HealthRequest) | [HealthResponse](#angzarr_client-proto-examples-HealthResponse) | Health check: reports model id, version, uptime, and request count. |
| GetActionsBatch | [BatchActionRequest](#angzarr_client-proto-examples-BatchActionRequest) | [BatchActionResponse](#angzarr_client-proto-examples-BatchActionResponse) | Batch inference for training/simulation (N requests -&gt; N responses). |
| StartSession | [StartSessionRequest](#angzarr_client-proto-examples-StartSessionRequest) | [StartSessionResponse](#angzarr_client-proto-examples-StartSessionResponse) | Start a training/play session. Sessions aggregate opponent statistics and bind a logical player_root to a session_id for experience recording. |
| EndSession | [EndSessionRequest](#angzarr_client-proto-examples-EndSessionRequest) | [EndSessionResponse](#angzarr_client-proto-examples-EndSessionResponse) | End a session; optionally persist accumulated opponent stats to storage. |
| RecordExperience | [Experience](#angzarr_client-proto-examples-Experience) | [RecordResponse](#angzarr_client-proto-examples-RecordResponse) | Record one experience tuple (state, action, reward) for replay / RL training. |
| GetOpponentStats | [OpponentQuery](#angzarr_client-proto-examples-OpponentQuery) | [OpponentStatsResponse](#angzarr_client-proto-examples-OpponentStatsResponse) | Query persistent opponent profiles by player_root. |
| ReloadModel | [ReloadModelRequest](#angzarr_client-proto-examples-ReloadModelRequest) | [ReloadModelResponse](#angzarr_client-proto-examples-ReloadModelResponse) | Reload model weights from a checkpoint path. Returns the new model version. Enables the offline trainer to publish updated weights without a restart. |

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/buy_in.proto {#angzarr_client_proto_examples_buy_in-proto}




### BuyInCompleted {#angzarr_client-proto-examples-BuyInCompleted}
PM state: buy-in flow completed successfully


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| table_root | bytes |  |  |
| reservation_id | bytes |  |  |
| seat | int32 |  |  |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| completed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### BuyInConfirmed {#angzarr_client-proto-examples-BuyInConfirmed}
Emitted when buy-in is confirmed


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| table_root | bytes |  |  |
| seat | int32 |  |  |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| confirmed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |







### BuyInFailed {#angzarr_client-proto-examples-BuyInFailed}
PM state: buy-in flow failed


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| table_root | bytes |  |  |
| reservation_id | bytes |  |  |
| failure | [OrchestrationFailure](#angzarr_client-proto-examples-OrchestrationFailure) |  |  |







### BuyInInitiated {#angzarr_client-proto-examples-BuyInInitiated}
PM state: buy-in flow initiated


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| table_root | bytes |  |  |
| reservation_id | bytes |  |  |
| seat | int32 |  |  |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| phase | [BuyInPhase](#angzarr_client-proto-examples-BuyInPhase) |  |  |
| initiated_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### BuyInOrchestratorState {#angzarr_client-proto-examples-BuyInOrchestratorState}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  | Correlation ID for this flow |
| player_root | bytes |  |  |
| table_root | bytes |  |  |
| seat | int32 |  |  |
| amount | int64 |  |  |
| phase | [BuyInPhase](#angzarr_client-proto-examples-BuyInPhase) |  |  |
| started_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### BuyInPhaseChanged {#angzarr_client-proto-examples-BuyInPhaseChanged}
PM state: phase transition


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| from_phase | [BuyInPhase](#angzarr_client-proto-examples-BuyInPhase) |  |  |
| to_phase | [BuyInPhase](#angzarr_client-proto-examples-BuyInPhase) |  |  |
| changed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### BuyInRequested {#angzarr_client-proto-examples-BuyInRequested}
Emitted when buy-in is requested - triggers PM


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  | UUID for this buy-in flow |
| table_root | bytes |  |  |
| seat | int32 |  |  |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| requested_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |







### BuyInReservationReleased {#angzarr_client-proto-examples-BuyInReservationReleased}
Emitted when buy-in reservation is released


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| reason | string |  |  |
| released_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |
| table_root | bytes |  |  |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |







### ConfirmBuyIn {#angzarr_client-proto-examples-ConfirmBuyIn}
PM confirms successful buy-in


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |







### InitiateBuyIn {#angzarr_client-proto-examples-InitiateBuyIn}
Client initiates buy-in - triggers PM orchestration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |
| seat | int32 |  | Preferred seat (-1 for any) |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| player_root | bytes |  |  |







### PlayerSeated {#angzarr_client-proto-examples-PlayerSeated}
Emitted when player is seated via PM flow


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| seat_position | int32 |  |  |
| stack | int64 |  |  |
| seated_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### ReleaseBuyIn {#angzarr_client-proto-examples-ReleaseBuyIn}
PM releases funds after failed seating


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| reason | string |  |  |







### SeatPlayer {#angzarr_client-proto-examples-SeatPlayer}
PM seats player at table (separate from JoinTable for PM flow)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  | Links to buy-in flow |
| seat | int32 |  | Preferred seat (-1 for any) |
| amount | int64 |  | Buy-in amount |







### SeatingRejected {#angzarr_client-proto-examples-SeatingRejected}
Emitted when seating is rejected


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| requested_seat | int32 |  |  |
| reason | string |  | &#34;seat_occupied&#34;, &#34;invalid_amount&#34;, etc. |
| rejected_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/hand.proto {#angzarr_client_proto_examples_hand-proto}




### ActionTaken {#angzarr_client-proto-examples-ActionTaken}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| action | [ActionType](#angzarr_client-proto-examples-ActionType) |  |  |
| amount | int64 |  |  |
| player_stack | int64 |  | Absolute stack after action |
| pot_total | int64 |  | Absolute pot after action |
| amount_to_call | int64 |  | Current call amount for next player |
| action_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### AwardPot {#angzarr_client-proto-examples-AwardPot}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| awards | [PotAward](#angzarr_client-proto-examples-PotAward) | repeated |  |







### BettingRoundComplete {#angzarr_client-proto-examples-BettingRoundComplete}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| completed_phase | [BettingPhase](#angzarr_client-proto-examples-BettingPhase) |  |  |
| pot_total | int64 |  |  |
| stacks | [PlayerStackSnapshot](#angzarr_client-proto-examples-PlayerStackSnapshot) | repeated |  |
| completed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### BlindPosted {#angzarr_client-proto-examples-BlindPosted}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| blind_type | string |  |  |
| amount | int64 |  |  |
| player_stack | int64 |  | Absolute stack after posting |
| pot_total | int64 |  | Absolute pot after posting |
| posted_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### CardsDealt {#angzarr_client-proto-examples-CardsDealt}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |
| hand_number | int64 |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| player_cards | [PlayerHoleCards](#angzarr_client-proto-examples-PlayerHoleCards) | repeated |  |
| dealer_position | int32 |  |  |
| players | [PlayerInHand](#angzarr_client-proto-examples-PlayerInHand) | repeated |  |
| dealt_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| remaining_deck | [Card](#angzarr_client-proto-examples-Card) | repeated | Cards left after dealing hole cards |







### CardsMucked {#angzarr_client-proto-examples-CardsMucked}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| mucked_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### CardsRevealed {#angzarr_client-proto-examples-CardsRevealed}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| ranking | [HandRanking](#angzarr_client-proto-examples-HandRanking) |  |  |
| revealed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### CommunityCardsDealt {#angzarr_client-proto-examples-CommunityCardsDealt}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| phase | [BettingPhase](#angzarr_client-proto-examples-BettingPhase) |  | FLOP, TURN, or RIVER |
| all_community_cards | [Card](#angzarr_client-proto-examples-Card) | repeated | Full board so far |
| dealt_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### DealCards {#angzarr_client-proto-examples-DealCards}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |
| hand_number | int64 |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| players | [PlayerInHand](#angzarr_client-proto-examples-PlayerInHand) | repeated |  |
| dealer_position | int32 |  |  |
| small_blind | int64 |  |  |
| big_blind | int64 |  |  |
| deck_seed | bytes |  | For deterministic shuffle (testing) |







### DealCommunityCards {#angzarr_client-proto-examples-DealCommunityCards}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | int32 |  | 3 for flop, 1 for turn/river |







### DrawCompleted {#angzarr_client-proto-examples-DrawCompleted}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| cards_discarded | int32 |  |  |
| cards_drawn | int32 |  |  |
| new_cards | [Card](#angzarr_client-proto-examples-Card) | repeated | Only visible to this player |
| drawn_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### HandComplete {#angzarr_client-proto-examples-HandComplete}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |
| hand_number | int64 |  |  |
| winners | [PotWinner](#angzarr_client-proto-examples-PotWinner) | repeated |  |
| final_stacks | [PlayerStackSnapshot](#angzarr_client-proto-examples-PlayerStackSnapshot) | repeated |  |
| completed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### HandState {#angzarr_client-proto-examples-HandState}
State (for snapshots)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hand_id | string |  |  |
| table_root | bytes |  |  |
| hand_number | int64 |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| remaining_deck | [Card](#angzarr_client-proto-examples-Card) | repeated | Deck state |
| players | [PlayerHandState](#angzarr_client-proto-examples-PlayerHandState) | repeated | Player state |
| community_cards | [Card](#angzarr_client-proto-examples-Card) | repeated | Community cards |
| current_phase | [BettingPhase](#angzarr_client-proto-examples-BettingPhase) |  | Betting state |
| action_on_position | int32 |  |  |
| current_bet | int64 |  |  |
| min_raise | int64 |  |  |
| pots | [Pot](#angzarr_client-proto-examples-Pot) | repeated |  |
| dealer_position | int32 |  | Positions |
| small_blind_position | int32 |  |  |
| big_blind_position | int32 |  |  |
| status | string |  | &#34;dealing&#34;, &#34;betting&#34;, &#34;showdown&#34;, &#34;complete&#34; |







### PlayerAction {#angzarr_client-proto-examples-PlayerAction}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| action | [ActionType](#angzarr_client-proto-examples-ActionType) |  |  |
| amount | int64 |  | For bet/raise/call |







### PlayerHandState {#angzarr_client-proto-examples-PlayerHandState}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| position | int32 |  |  |
| hole_cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| stack | int64 |  |  |
| bet_this_round | int64 |  |  |
| total_invested | int64 |  |  |
| has_acted | bool |  |  |
| has_folded | bool |  |  |
| is_all_in | bool |  |  |







### PlayerHoleCards {#angzarr_client-proto-examples-PlayerHoleCards}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |







### PlayerInHand {#angzarr_client-proto-examples-PlayerInHand}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| position | int32 |  |  |
| stack | int64 |  |  |







### PlayerStackSnapshot {#angzarr_client-proto-examples-PlayerStackSnapshot}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| stack | int64 |  |  |
| is_all_in | bool |  |  |
| has_folded | bool |  |  |







### PlayerTimedOut {#angzarr_client-proto-examples-PlayerTimedOut}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| default_action | [ActionType](#angzarr_client-proto-examples-ActionType) |  | Usually FOLD or CHECK |
| timed_out_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PostBlind {#angzarr_client-proto-examples-PostBlind}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| blind_type | string |  | &#34;small&#34;, &#34;big&#34;, &#34;ante&#34; |
| amount | int64 |  |  |







### PotAward {#angzarr_client-proto-examples-PotAward}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| amount | int64 |  |  |
| pot_type | string |  |  |







### PotAwarded {#angzarr_client-proto-examples-PotAwarded}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| winners | [PotWinner](#angzarr_client-proto-examples-PotWinner) | repeated |  |
| awarded_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PotWinner {#angzarr_client-proto-examples-PotWinner}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| amount | int64 |  |  |
| pot_type | string |  |  |
| winning_hand | [HandRanking](#angzarr_client-proto-examples-HandRanking) |  |  |







### RequestDraw {#angzarr_client-proto-examples-RequestDraw}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| card_indices | int32 | repeated | Which cards to discard (0-indexed) |







### RevealCards {#angzarr_client-proto-examples-RevealCards}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| muck | bool |  | True to hide cards (fold at showdown) |







### ShowdownStarted {#angzarr_client-proto-examples-ShowdownStarted}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| players_to_show | bytes | repeated | Order of revelation |
| started_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/orchestration.proto {#angzarr_client_proto_examples_orchestration-proto}




### OrchestrationFailure {#angzarr_client-proto-examples-OrchestrationFailure}
Reason for orchestration failure


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| code | string |  | Machine-readable code |
| message | string |  | Human-readable message |
| failed_at_phase | string |  | Which phase failed |
| failed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |





 



### BuyInPhase {#angzarr_client-proto-examples-BuyInPhase}
Phase tracking for buy-in orchestration

| Name | Number | Description |
| ---- | ------ | ----------- |
| BUY_IN_PHASE_UNSPECIFIED | 0 |  |
| BUY_IN_REQUESTED | 1 | Initial request received |
| BUY_IN_RESERVING | 2 | Funds being reserved |
| BUY_IN_SEATING | 3 | Awaiting seat confirmation |
| BUY_IN_CONFIRMING | 4 | Confirming with player |
| BUY_IN_COMPLETED | 5 | Successfully completed |
| BUY_IN_FAILED | 6 | Failed - funds released |




### RebuyPhase {#angzarr_client-proto-examples-RebuyPhase}
Phase tracking for rebuy orchestration

| Name | Number | Description |
| ---- | ------ | ----------- |
| REBUY_PHASE_UNSPECIFIED | 0 |  |
| REBUY_REQUESTED | 1 | Initial request received |
| REBUY_RESERVING | 2 | Fee being reserved |
| REBUY_APPROVING | 3 | Awaiting tournament approval |
| REBUY_ADDING_CHIPS | 4 | Adding chips to table |
| REBUY_CONFIRMING | 5 | Confirming with player |
| REBUY_COMPLETED | 6 | Successfully completed |
| REBUY_FAILED | 7 | Failed - fee released |




### RegistrationPhase {#angzarr_client-proto-examples-RegistrationPhase}
Phase tracking for tournament registration

| Name | Number | Description |
| ---- | ------ | ----------- |
| REGISTRATION_PHASE_UNSPECIFIED | 0 |  |
| REGISTRATION_REQUESTED | 1 | Initial request received |
| REGISTRATION_RESERVING | 2 | Fee being reserved |
| REGISTRATION_ENROLLING | 3 | Awaiting tournament confirmation |
| REGISTRATION_CONFIRMING | 4 | Confirming with player |
| REGISTRATION_COMPLETED | 5 | Successfully registered |
| REGISTRATION_FAILED | 6 | Failed - fee released |


 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/player.proto {#angzarr_client_proto_examples_player-proto}




### ActionRequested {#angzarr_client-proto-examples-ActionRequested}
Emitted when action is needed - AI players respond via sidecar


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hand_root | bytes |  |  |
| table_root | bytes |  |  |
| player_root | bytes |  |  |
| player_type | [PlayerType](#angzarr_client-proto-examples-PlayerType) |  |  |
| amount_to_call | int64 |  |  |
| min_raise | int64 |  |  |
| max_raise | int64 |  |  |
| hole_cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| community_cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| pot_size | int64 |  |  |
| phase | [BettingPhase](#angzarr_client-proto-examples-BettingPhase) |  |  |
| deadline | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### DeductReservedFunds {#angzarr_client-proto-examples-DeductReservedFunds}
Settle a previously-reserved amount: reserved_funds AND bankroll both drop.
Emitted by the reservation PM after a confirmed lifecycle action
(BuyInConfirmed / RebuyFeeConfirmed / RegistrationFeeConfirmed).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| key | bytes |  |  |
| reservation_id | bytes |  |  |







### DepositFunds {#angzarr_client-proto-examples-DepositFunds}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |







### FundsDeducted {#angzarr_client-proto-examples-FundsDeducted}
Emitted after DeductReservedFunds — settles a reservation permanently.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| key | bytes |  |  |
| reservation_id | bytes |  |  |
| new_balance | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| new_reserved_balance | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| deducted_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### FundsDeposited {#angzarr_client-proto-examples-FundsDeposited}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| new_balance | [Currency](#angzarr_client-proto-examples-Currency) |  | Absolute value after deposit |
| deposited_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### FundsReleased {#angzarr_client-proto-examples-FundsReleased}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| key | bytes |  |  |
| new_available_balance | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| new_reserved_balance | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| released_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### FundsReserved {#angzarr_client-proto-examples-FundsReserved}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| key | bytes |  |  |
| new_available_balance | [Currency](#angzarr_client-proto-examples-Currency) |  | Bankroll minus reserved |
| new_reserved_balance | [Currency](#angzarr_client-proto-examples-Currency) |  | Total reserved across all keys |
| reserved_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### FundsTransferred {#angzarr_client-proto-examples-FundsTransferred}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from_player_root | bytes |  |  |
| to_player_root | bytes |  |  |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| hand_root | bytes |  |  |
| reason | string |  |  |
| new_balance | [Currency](#angzarr_client-proto-examples-Currency) |  | Recipient&#39;s new balance |
| transferred_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### FundsWithdrawn {#angzarr_client-proto-examples-FundsWithdrawn}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| new_balance | [Currency](#angzarr_client-proto-examples-Currency) |  | Absolute value after withdrawal |
| withdrawn_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerRegistered {#angzarr_client-proto-examples-PlayerRegistered}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| display_name | string |  |  |
| email | string |  |  |
| player_type | [PlayerType](#angzarr_client-proto-examples-PlayerType) |  |  |
| ai_model_id | string |  |  |
| registered_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerReturningToPlay {#angzarr_client-proto-examples-PlayerReturningToPlay}
Player has chosen to return to play at a table


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |
| sat_in_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerSittingOut {#angzarr_client-proto-examples-PlayerSittingOut}
Player has chosen to sit out at a table


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |
| sat_out_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerState {#angzarr_client-proto-examples-PlayerState}
State (for snapshots)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_id | string |  |  |
| display_name | string |  |  |
| email | string |  |  |
| player_type | [PlayerType](#angzarr_client-proto-examples-PlayerType) |  |  |
| ai_model_id | string |  |  |
| bankroll | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| reserved_funds | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| table_reservations | [PlayerState.TableReservationsEntry](#angzarr_client-proto-examples-PlayerState-TableReservationsEntry) | repeated | key_hex -&gt; amount (legacy name retained for on-disk compatibility) |
| status | string |  | &#34;active&#34;, &#34;suspended&#34;, etc. |







### PlayerState.TableReservationsEntry {#angzarr_client-proto-examples-PlayerState-TableReservationsEntry}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | string |  |  |
| value | int64 |  |  |







### RegisterPlayer {#angzarr_client-proto-examples-RegisterPlayer}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| display_name | string |  |  |
| email | string |  | Used for root derivation |
| player_type | [PlayerType](#angzarr_client-proto-examples-PlayerType) |  | HUMAN or AI |
| ai_model_id | string |  | For AI players: which model to use |







### ReleaseFunds {#angzarr_client-proto-examples-ReleaseFunds}
Release reserved funds back to bankroll (cancel / compensate).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | bytes |  |  |







### RequestAction {#angzarr_client-proto-examples-RequestAction}
Request action from player (triggers AI sidecar for AI players)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hand_root | bytes |  |  |
| table_root | bytes |  |  |
| amount_to_call | int64 |  |  |
| min_raise | int64 |  |  |
| max_raise | int64 |  | Player&#39;s remaining stack |
| hole_cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| community_cards | [Card](#angzarr_client-proto-examples-Card) | repeated |  |
| pot_size | int64 |  |  |
| phase | [BettingPhase](#angzarr_client-proto-examples-BettingPhase) |  |  |
| timeout_seconds | int32 |  |  |







### ReserveFunds {#angzarr_client-proto-examples-ReserveFunds}
Reserve funds for a lifecycle action (buy-in, rebuy, tournament registration).
``key`` identifies the reservation bucket — a table_root for buy-in / rebuy
flows, a tournament_root for registration flows. Matches the ``key`` field on
``DeductReservedFunds`` / ``FundsDeducted``.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| key | bytes |  |  |







### SitIn {#angzarr_client-proto-examples-SitIn}
Player decides to return to play at a table


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |







### SitOut {#angzarr_client-proto-examples-SitOut}
Player decides to sit out at a table (stop receiving hands)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_root | bytes |  |  |







### TransferFunds {#angzarr_client-proto-examples-TransferFunds}
Transfer funds from one player to another (pot award)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from_player_root | bytes |  | Source player (for reserved funds) |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| hand_root | bytes |  | Which hand this transfer is for |
| reason | string |  | &#34;pot_win&#34;, &#34;side_pot_win&#34;, etc. |







### WithdrawFunds {#angzarr_client-proto-examples-WithdrawFunds}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [Currency](#angzarr_client-proto-examples-Currency) |  |  |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/poker_types.proto {#angzarr_client_proto_examples_poker_types-proto}




### Card {#angzarr_client-proto-examples-Card}
Card representation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| suit | [Suit](#angzarr_client-proto-examples-Suit) |  |  |
| rank | [Rank](#angzarr_client-proto-examples-Rank) |  |  |







### Currency {#angzarr_client-proto-examples-Currency}
Currency amount (in smallest unit, e.g., cents)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | int64 |  |  |
| currency_code | string |  | &#34;USD&#34;, &#34;EUR&#34;, &#34;CHIPS&#34; |







### HandRanking {#angzarr_client-proto-examples-HandRanking}
Hand ranking result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rank_type | [HandRankType](#angzarr_client-proto-examples-HandRankType) |  |  |
| kickers | [Rank](#angzarr_client-proto-examples-Rank) | repeated | For tie-breaking |
| score | int32 |  | Numeric score for comparison |







### Pot {#angzarr_client-proto-examples-Pot}
Pot structure (for side pots)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | int64 |  |  |
| eligible_players | bytes | repeated | Player roots eligible for this pot |
| pot_type | string |  | &#34;main&#34; or &#34;side_N&#34; |







### Seat {#angzarr_client-proto-examples-Seat}
Position at table


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| position | int32 |  | 0-9 for 10-max table |
| player_root | bytes |  | Player aggregate root |
| stack | [Currency](#angzarr_client-proto-examples-Currency) |  | Current stack at table |
| is_active | bool |  | Still in current hand |
| is_sitting_out | bool |  | Temporarily away |





 



### ActionType {#angzarr_client-proto-examples-ActionType}
Player action type

| Name | Number | Description |
| ---- | ------ | ----------- |
| ACTION_UNSPECIFIED | 0 |  |
| FOLD | 1 |  |
| CHECK | 2 |  |
| CALL | 3 |  |
| BET | 4 |  |
| RAISE | 5 |  |
| ALL_IN | 6 |  |




### BettingPhase {#angzarr_client-proto-examples-BettingPhase}
Betting round phase

| Name | Number | Description |
| ---- | ------ | ----------- |
| BETTING_PHASE_UNSPECIFIED | 0 |  |
| PREFLOP | 1 |  |
| FLOP | 2 |  |
| TURN | 3 |  |
| RIVER | 4 |  |
| DRAW | 5 | For draw games |
| SHOWDOWN | 6 |  |




### GameVariant {#angzarr_client-proto-examples-GameVariant}
Game variant configuration

| Name | Number | Description |
| ---- | ------ | ----------- |
| GAME_VARIANT_UNSPECIFIED | 0 |  |
| TEXAS_HOLDEM | 1 |  |
| OMAHA | 2 |  |
| FIVE_CARD_DRAW | 3 |  |
| SEVEN_CARD_STUD | 4 |  |




### HandRankType {#angzarr_client-proto-examples-HandRankType}


| Name | Number | Description |
| ---- | ------ | ----------- |
| HAND_RANK_UNSPECIFIED | 0 |  |
| HIGH_CARD | 1 |  |
| PAIR | 2 |  |
| TWO_PAIR | 3 |  |
| THREE_OF_A_KIND | 4 |  |
| STRAIGHT | 5 |  |
| FLUSH | 6 |  |
| FULL_HOUSE | 7 |  |
| FOUR_OF_A_KIND | 8 |  |
| STRAIGHT_FLUSH | 9 |  |
| ROYAL_FLUSH | 10 |  |




### PlayerType {#angzarr_client-proto-examples-PlayerType}
Player type - abstraction for human vs AI

| Name | Number | Description |
| ---- | ------ | ----------- |
| PLAYER_TYPE_UNSPECIFIED | 0 |  |
| HUMAN | 1 |  |
| AI | 2 |  |




### Rank {#angzarr_client-proto-examples-Rank}


| Name | Number | Description |
| ---- | ------ | ----------- |
| RANK_UNSPECIFIED | 0 |  |
| TWO | 2 |  |
| THREE | 3 |  |
| FOUR | 4 |  |
| FIVE | 5 |  |
| SIX | 6 |  |
| SEVEN | 7 |  |
| EIGHT | 8 |  |
| NINE | 9 |  |
| TEN | 10 |  |
| JACK | 11 |  |
| QUEEN | 12 |  |
| KING | 13 |  |
| ACE | 14 |  |




### Suit {#angzarr_client-proto-examples-Suit}


| Name | Number | Description |
| ---- | ------ | ----------- |
| SUIT_UNSPECIFIED | 0 |  |
| CLUBS | 1 |  |
| DIAMONDS | 2 |  |
| HEARTS | 3 |  |
| SPADES | 4 |  |


 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/rebuy.proto {#angzarr_client_proto_examples_rebuy-proto}




### AddRebuyChips {#angzarr_client-proto-examples-AddRebuyChips}
PM adds chips to player&#39;s stack (fact-like - no validation)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| seat | int32 |  |  |
| amount | int64 |  |  |







### ConfirmRebuyFee {#angzarr_client-proto-examples-ConfirmRebuyFee}
PM confirms successful rebuy


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |







### InitiateRebuy {#angzarr_client-proto-examples-InitiateRebuy}
Client initiates rebuy - triggers PM orchestration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tournament_root | bytes |  |  |
| table_root | bytes |  |  |
| seat | int32 |  | Player&#39;s seat at table |
| player_root | bytes |  |  |







### RebuyChipsAdded {#angzarr_client-proto-examples-RebuyChipsAdded}
Emitted when rebuy chips are added to player&#39;s stack


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| seat | int32 |  |  |
| amount | int64 |  |  |
| new_stack | int64 |  |  |
| added_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RebuyCompleted {#angzarr_client-proto-examples-RebuyCompleted}
PM state: rebuy flow completed successfully


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| table_root | bytes |  |  |
| reservation_id | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| chips_added | int64 |  |  |
| completed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RebuyFailed {#angzarr_client-proto-examples-RebuyFailed}
PM state: rebuy flow failed


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| reservation_id | bytes |  |  |
| failure | [OrchestrationFailure](#angzarr_client-proto-examples-OrchestrationFailure) |  |  |







### RebuyFeeConfirmed {#angzarr_client-proto-examples-RebuyFeeConfirmed}
Emitted when rebuy fee is confirmed


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| tournament_root | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| chips_added | int64 |  |  |
| confirmed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |
| table_root | bytes |  |  |







### RebuyFeeReleased {#angzarr_client-proto-examples-RebuyFeeReleased}
Emitted when rebuy fee is released


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| reason | string |  |  |
| released_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| table_root | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |







### RebuyInitiated {#angzarr_client-proto-examples-RebuyInitiated}
PM state: rebuy flow initiated


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| table_root | bytes |  |  |
| reservation_id | bytes |  |  |
| seat | int32 |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| chips_to_add | int64 |  |  |
| phase | [RebuyPhase](#angzarr_client-proto-examples-RebuyPhase) |  |  |
| initiated_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RebuyOrchestratorState {#angzarr_client-proto-examples-RebuyOrchestratorState}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  | Correlation ID for this flow |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| table_root | bytes |  |  |
| seat | int32 |  |  |
| fee | int64 |  |  |
| chips_to_add | int64 |  |  |
| phase | [RebuyPhase](#angzarr_client-proto-examples-RebuyPhase) |  |  |
| started_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RebuyPhaseChanged {#angzarr_client-proto-examples-RebuyPhaseChanged}
PM state: phase transition


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| from_phase | [RebuyPhase](#angzarr_client-proto-examples-RebuyPhase) |  |  |
| to_phase | [RebuyPhase](#angzarr_client-proto-examples-RebuyPhase) |  |  |
| changed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RebuyRequested {#angzarr_client-proto-examples-RebuyRequested}
Emitted when rebuy is requested - triggers PM


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  | UUID for this rebuy flow |
| tournament_root | bytes |  |  |
| table_root | bytes |  |  |
| seat | int32 |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  | Looked up from tournament rebuy config |
| requested_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |







### ReleaseRebuyFee {#angzarr_client-proto-examples-ReleaseRebuyFee}
PM releases fee after failed rebuy


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| reason | string |  |  |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/registration.proto {#angzarr_client_proto_examples_registration-proto}




### ConfirmRegistrationFee {#angzarr_client-proto-examples-ConfirmRegistrationFee}
PM confirms successful registration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |







### InitiateTournamentRegistration {#angzarr_client-proto-examples-InitiateTournamentRegistration}
Client initiates tournament registration - triggers PM orchestration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tournament_root | bytes |  |  |
| player_root | bytes |  |  |







### RegistrationCompleted {#angzarr_client-proto-examples-RegistrationCompleted}
PM state: registration flow completed successfully


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| reservation_id | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| starting_stack | int64 |  |  |
| completed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RegistrationFailed {#angzarr_client-proto-examples-RegistrationFailed}
PM state: registration flow failed


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| reservation_id | bytes |  |  |
| failure | [OrchestrationFailure](#angzarr_client-proto-examples-OrchestrationFailure) |  |  |







### RegistrationFeeConfirmed {#angzarr_client-proto-examples-RegistrationFeeConfirmed}
Emitted when registration fee is confirmed


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| tournament_root | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| confirmed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |







### RegistrationFeeReleased {#angzarr_client-proto-examples-RegistrationFeeReleased}
Emitted when registration fee is released


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| reason | string |  |  |
| released_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |







### RegistrationInitiated {#angzarr_client-proto-examples-RegistrationInitiated}
PM state: registration flow initiated


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| reservation_id | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  |  |
| phase | [RegistrationPhase](#angzarr_client-proto-examples-RegistrationPhase) |  |  |
| initiated_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RegistrationOrchestratorState {#angzarr_client-proto-examples-RegistrationOrchestratorState}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  | Correlation ID for this flow |
| player_root | bytes |  |  |
| tournament_root | bytes |  |  |
| fee | int64 |  |  |
| phase | [RegistrationPhase](#angzarr_client-proto-examples-RegistrationPhase) |  |  |
| started_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RegistrationPhaseChanged {#angzarr_client-proto-examples-RegistrationPhaseChanged}
PM state: phase transition


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| from_phase | [RegistrationPhase](#angzarr_client-proto-examples-RegistrationPhase) |  |  |
| to_phase | [RegistrationPhase](#angzarr_client-proto-examples-RegistrationPhase) |  |  |
| changed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RegistrationRequested {#angzarr_client-proto-examples-RegistrationRequested}
Emitted when registration is requested - triggers PM


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  | UUID for this registration flow |
| tournament_root | bytes |  |  |
| fee | [Currency](#angzarr_client-proto-examples-Currency) |  | Looked up from tournament |
| requested_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| player_root | bytes |  |  |







### ReleaseRegistrationFee {#angzarr_client-proto-examples-ReleaseRegistrationFee}
PM releases fee after failed registration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reservation_id | bytes |  |  |
| reason | string |  |  |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/table.proto {#angzarr_client_proto_examples_table-proto}




### AddChips {#angzarr_client-proto-examples-AddChips}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| amount | int64 |  |  |







### ChipsAdded {#angzarr_client-proto-examples-ChipsAdded}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| amount | int64 |  |  |
| new_stack | int64 |  | Absolute stack after add |
| added_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### CreateTable {#angzarr_client-proto-examples-CreateTable}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_name | string |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| small_blind | int64 |  |  |
| big_blind | int64 |  |  |
| min_buy_in | int64 |  |  |
| max_buy_in | int64 |  |  |
| max_players | int32 |  | 2-10 |
| action_timeout_seconds | int32 |  |  |







### EndHand {#angzarr_client-proto-examples-EndHand}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hand_root | bytes |  |  |
| results | [PotResult](#angzarr_client-proto-examples-PotResult) | repeated |  |







### HandEnded {#angzarr_client-proto-examples-HandEnded}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hand_root | bytes |  |  |
| results | [PotResult](#angzarr_client-proto-examples-PotResult) | repeated |  |
| stack_changes | [HandEnded.StackChangesEntry](#angzarr_client-proto-examples-HandEnded-StackChangesEntry) | repeated | player_root_hex -&gt; delta |
| ended_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### HandEnded.StackChangesEntry {#angzarr_client-proto-examples-HandEnded-StackChangesEntry}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | string |  |  |
| value | int64 |  |  |







### HandStarted {#angzarr_client-proto-examples-HandStarted}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hand_root | bytes |  | New hand aggregate root |
| hand_number | int64 |  |  |
| dealer_position | int32 |  |  |
| small_blind_position | int32 |  |  |
| big_blind_position | int32 |  |  |
| active_players | [SeatSnapshot](#angzarr_client-proto-examples-SeatSnapshot) | repeated |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| small_blind | int64 |  |  |
| big_blind | int64 |  |  |
| started_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### JoinTable {#angzarr_client-proto-examples-JoinTable}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| preferred_seat | int32 |  | -1 for any available |
| buy_in_amount | int64 |  |  |







### LeaveTable {#angzarr_client-proto-examples-LeaveTable}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |







### PlayerJoined {#angzarr_client-proto-examples-PlayerJoined}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| seat_position | int32 |  |  |
| buy_in_amount | int64 |  |  |
| stack | int64 |  | Absolute stack after buy-in |
| joined_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerLeft {#angzarr_client-proto-examples-PlayerLeft}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| seat_position | int32 |  |  |
| chips_cashed_out | int64 |  |  |
| left_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerSatIn {#angzarr_client-proto-examples-PlayerSatIn}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| sat_in_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerSatOut {#angzarr_client-proto-examples-PlayerSatOut}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| sat_out_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PotResult {#angzarr_client-proto-examples-PotResult}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| winner_root | bytes |  |  |
| amount | int64 |  |  |
| pot_type | string |  | &#34;main&#34; or &#34;side_N&#34; |
| winning_hand | [HandRanking](#angzarr_client-proto-examples-HandRanking) |  |  |







### SeatSnapshot {#angzarr_client-proto-examples-SeatSnapshot}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| position | int32 |  |  |
| player_root | bytes |  |  |
| stack | int64 |  |  |







### StartHand {#angzarr_client-proto-examples-StartHand}
No parameters - uses current table state
Dealer button advances automatically







### TableCreated {#angzarr_client-proto-examples-TableCreated}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_name | string |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| small_blind | int64 |  |  |
| big_blind | int64 |  |  |
| min_buy_in | int64 |  |  |
| max_buy_in | int64 |  |  |
| max_players | int32 |  |  |
| action_timeout_seconds | int32 |  |  |
| created_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TableState {#angzarr_client-proto-examples-TableState}
State (for snapshots)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table_id | string |  |  |
| table_name | string |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| small_blind | int64 |  |  |
| big_blind | int64 |  |  |
| min_buy_in | int64 |  |  |
| max_buy_in | int64 |  |  |
| max_players | int32 |  |  |
| action_timeout_seconds | int32 |  |  |
| seats | [Seat](#angzarr_client-proto-examples-Seat) | repeated |  |
| dealer_position | int32 |  |  |
| hand_count | int64 |  |  |
| current_hand_root | bytes |  |  |
| status | string |  | &#34;waiting&#34;, &#34;in_hand&#34;, &#34;paused&#34; |





 

 

 

 



<p align="right"><a href="#top">Top</a></p>

## angzarr_client/proto/examples/tournament.proto {#angzarr_client_proto_examples_tournament-proto}




### AddonConfig {#angzarr_client-proto-examples-AddonConfig}
Addon configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | bool |  |  |
| addon_level | int32 |  | Level when addon is available |
| addon_cost | int64 |  |  |
| addon_chips | int64 |  |  |







### AddonProcessed {#angzarr_client-proto-examples-AddonProcessed}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| addon_cost | int64 |  |  |
| chips_added | int64 |  |  |
| processed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### AdvanceBlindLevel {#angzarr_client-proto-examples-AdvanceBlindLevel}
Advance to next blind level

No parameters - advances to next level in structure







### BlindLevel {#angzarr_client-proto-examples-BlindLevel}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| level | int32 |  |  |
| small_blind | int64 |  |  |
| big_blind | int64 |  |  |
| ante | int64 |  |  |
| duration_minutes | int32 |  |  |







### BlindLevelAdvanced {#angzarr_client-proto-examples-BlindLevelAdvanced}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| level | int32 |  |  |
| small_blind | int64 |  |  |
| big_blind | int64 |  |  |
| ante | int64 |  |  |
| advanced_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### CloseRegistration {#angzarr_client-proto-examples-CloseRegistration}
No parameters







### CompleteTournament {#angzarr_client-proto-examples-CompleteTournament}
Complete the tournament — records the winner and closes the tournament.
Typically issued after the last elimination leaves one player remaining.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| winner_root | bytes |  |  |







### CreateTournament {#angzarr_client-proto-examples-CreateTournament}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | string |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| buy_in | int64 |  |  |
| starting_stack | int64 |  |  |
| max_players | int32 |  |  |
| min_players | int32 |  |  |
| scheduled_start | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| rebuy_config | [RebuyConfig](#angzarr_client-proto-examples-RebuyConfig) |  |  |
| addon_config | [AddonConfig](#angzarr_client-proto-examples-AddonConfig) |  |  |
| blind_structure | [BlindLevel](#angzarr_client-proto-examples-BlindLevel) | repeated |  |







### EliminatePlayer {#angzarr_client-proto-examples-EliminatePlayer}
Eliminate a player (from table event via saga)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| hand_root | bytes |  | Hand where elimination occurred |







### EnrollPlayer {#angzarr_client-proto-examples-EnrollPlayer}
PM sends this command to register a player in tournament


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  | Links to registration flow |







### OpenRegistration {#angzarr_client-proto-examples-OpenRegistration}
No parameters - uses tournament config







### PauseTournament {#angzarr_client-proto-examples-PauseTournament}
Pause the tournament (break)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reason | string |  |  |







### PlayerEliminated {#angzarr_client-proto-examples-PlayerEliminated}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| finish_position | int32 |  |  |
| hand_root | bytes |  |  |
| payout | int64 |  | 0 if out of the money |
| eliminated_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerRegistration {#angzarr_client-proto-examples-PlayerRegistration}
Player registration record


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| fee_paid | int64 |  |  |
| starting_stack | int64 |  |  |
| rebuys_used | int32 |  |  |
| addon_taken | bool |  |  |
| table_assignment | int32 |  | Table number |
| seat_assignment | int32 |  |  |
| registered_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### PlayerUnregistered {#angzarr_client-proto-examples-PlayerUnregistered}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| refund_amount | int64 |  |  |
| unregistered_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### ProcessAddon {#angzarr_client-proto-examples-ProcessAddon}
PM sends this command to process an addon


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |







### ProcessRebuy {#angzarr_client-proto-examples-ProcessRebuy}
PM sends this command to process a rebuy


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  | Links to rebuy flow |







### RebuyConfig {#angzarr_client-proto-examples-RebuyConfig}
Rebuy configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | bool |  |  |
| max_rebuys | int32 |  | 0 = unlimited |
| rebuy_level_cutoff | int32 |  | Last level allowing rebuys |
| stack_threshold | int64 |  | Max stack to be eligible |
| rebuy_cost | int64 |  |  |
| rebuy_chips | int64 |  |  |







### RebuyDenied {#angzarr_client-proto-examples-RebuyDenied}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| reason | string |  | &#34;window_closed&#34;, &#34;stack_too_high&#34;, &#34;max_reached&#34; |
| denied_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RebuyProcessed {#angzarr_client-proto-examples-RebuyProcessed}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| rebuy_cost | int64 |  |  |
| chips_added | int64 |  |  |
| rebuy_count | int32 |  | Player&#39;s total rebuys |
| processed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RegistrationClosed {#angzarr_client-proto-examples-RegistrationClosed}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| total_registrations | int32 |  |  |
| closed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### RegistrationOpened {#angzarr_client-proto-examples-RegistrationOpened}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| opened_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### ResumeTournament {#angzarr_client-proto-examples-ResumeTournament}
Resume the tournament

No parameters







### StartTournament {#angzarr_client-proto-examples-StartTournament}
Start the tournament (requires min players)

No parameters







### TournamentCompleted {#angzarr_client-proto-examples-TournamentCompleted}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| winner_root | bytes |  |  |
| total_prize_pool | int64 |  |  |
| results | [TournamentResult](#angzarr_client-proto-examples-TournamentResult) | repeated |  |
| completed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TournamentCreated {#angzarr_client-proto-examples-TournamentCreated}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | string |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| buy_in | int64 |  |  |
| starting_stack | int64 |  |  |
| max_players | int32 |  |  |
| min_players | int32 |  |  |
| scheduled_start | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| rebuy_config | [RebuyConfig](#angzarr_client-proto-examples-RebuyConfig) |  |  |
| addon_config | [AddonConfig](#angzarr_client-proto-examples-AddonConfig) |  |  |
| blind_structure | [BlindLevel](#angzarr_client-proto-examples-BlindLevel) | repeated |  |
| created_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TournamentEnrollmentRejected {#angzarr_client-proto-examples-TournamentEnrollmentRejected}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| reason | string |  | &#34;full&#34;, &#34;closed&#34;, &#34;already_registered&#34; |
| rejected_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TournamentPaused {#angzarr_client-proto-examples-TournamentPaused}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reason | string |  |  |
| paused_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TournamentPlayerEnrolled {#angzarr_client-proto-examples-TournamentPlayerEnrolled}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |
| reservation_id | bytes |  |  |
| fee_paid | int64 |  |  |
| starting_stack | int64 |  |  |
| registration_number | int32 |  | Order of registration |
| enrolled_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TournamentResult {#angzarr_client-proto-examples-TournamentResult}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| position | int32 |  |  |
| player_root | bytes |  |  |
| payout | int64 |  |  |







### TournamentResumed {#angzarr_client-proto-examples-TournamentResumed}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resumed_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TournamentStarted {#angzarr_client-proto-examples-TournamentStarted}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| total_players | int32 |  |  |
| tables_created | int32 |  |  |
| total_prize_pool | int64 |  |  |
| started_at | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |







### TournamentState {#angzarr_client-proto-examples-TournamentState}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tournament_id | string |  |  |
| name | string |  |  |
| game_variant | [GameVariant](#angzarr_client-proto-examples-GameVariant) |  |  |
| status | [TournamentStatus](#angzarr_client-proto-examples-TournamentStatus) |  |  |
| buy_in | int64 |  |  |
| starting_stack | int64 |  |  |
| max_players | int32 |  |  |
| min_players | int32 |  |  |
| scheduled_start | [google.protobuf.Timestamp](https://protobuf.dev/reference/protobuf/google.protobuf/#timestamp) |  |  |
| rebuy_config | [RebuyConfig](#angzarr_client-proto-examples-RebuyConfig) |  |  |
| addon_config | [AddonConfig](#angzarr_client-proto-examples-AddonConfig) |  |  |
| blind_structure | [BlindLevel](#angzarr_client-proto-examples-BlindLevel) | repeated |  |
| current_level | int32 |  |  |
| registered_players | [TournamentState.RegisteredPlayersEntry](#angzarr_client-proto-examples-TournamentState-RegisteredPlayersEntry) | repeated | player_root_hex -&gt; registration |
| players_remaining | int32 |  |  |
| total_prize_pool | int64 |  |  |







### TournamentState.RegisteredPlayersEntry {#angzarr_client-proto-examples-TournamentState-RegisteredPlayersEntry}



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | string |  |  |
| value | [PlayerRegistration](#angzarr_client-proto-examples-PlayerRegistration) |  |  |







### UnregisterPlayer {#angzarr_client-proto-examples-UnregisterPlayer}
Player requests to unregister (refund if allowed)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| player_root | bytes |  |  |





 



### TournamentStatus {#angzarr_client-proto-examples-TournamentStatus}
Tournament status

| Name | Number | Description |
| ---- | ------ | ----------- |
| TOURNAMENT_STATUS_UNSPECIFIED | 0 |  |
| TOURNAMENT_CREATED | 1 | Created but not open |
| TOURNAMENT_REGISTRATION_OPEN | 2 | Accepting registrations |
| TOURNAMENT_RUNNING | 3 | In progress |
| TOURNAMENT_PAUSED | 4 | On break |
| TOURNAMENT_COMPLETED | 5 | Finished |
| TOURNAMENT_CANCELLED | 6 | Cancelled |


 

 

 



<p align="right"><a href="#top">Top</a></p>

## google/api/annotations.proto {#google_api_annotations-proto}


 

 



### File-level Extensions {#google_api_annotations-proto-extensions}
| Extension | Type | Base | Number | Description |
| --------- | ---- | ---- | ------ | ----------- |
| http | HttpRule | .google.protobuf.MethodOptions | 72295728 | See `HttpRule`. |

 

 



<p align="right"><a href="#top">Top</a></p>

## google/api/http.proto {#google_api_http-proto}




### CustomHttpPattern {#google-api-CustomHttpPattern}
A custom pattern is used for defining custom HTTP verb.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | string |  | The name of this custom HTTP verb. |
| path | string |  | The path matched by this custom verb. |







### Http {#google-api-Http}
Defines the HTTP configuration for an API service. It contains a list of
[HttpRule][google.api.HttpRule], each specifying the mapping of an RPC method
to one or more HTTP REST API methods.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rules | [HttpRule](#google-api-HttpRule) | repeated | A list of HTTP configuration rules that apply to individual API methods.

**NOTE:** All service configuration rules follow &#34;last one wins&#34; order. |
| fully_decode_reserved_expansion | bool |  | When set to true, URL path parameters will be fully URI-decoded except in cases of single segment matches in reserved expansion, where &#34;%2F&#34; will be left encoded.

The default behavior is to not decode RFC 6570 reserved characters in multi segment matches. |







### HttpRule {#google-api-HttpRule}
gRPC Transcoding

gRPC Transcoding is a feature for mapping between a gRPC method and one or
more HTTP REST endpoints. It allows developers to build a single API service
that supports both gRPC APIs and REST APIs. Many systems, including [Google
APIs](https://github.com/googleapis/googleapis),
[Cloud Endpoints](https://cloud.google.com/endpoints), [gRPC
Gateway](https://github.com/grpc-ecosystem/grpc-gateway),
and [Envoy](https://github.com/envoyproxy/envoy) proxy support this feature
and use it for large scale production services.

`HttpRule` defines the schema of the gRPC/REST mapping. The mapping specifies
how different portions of the gRPC request message are mapped to the URL
path, URL query parameters, and HTTP request body. It also controls how the
gRPC response message is mapped to the HTTP response body. `HttpRule` is
typically specified as an `google.api.http` annotation on the gRPC method.

Each mapping specifies a URL path template and an HTTP method. The path
template may refer to one or more fields in the gRPC request message, as long
as each field is a non-repeated field with a primitive (non-message) type.
The path template controls how fields of the request message are mapped to
the URL path.

Example:

    service Messaging \{
      rpc GetMessage(GetMessageRequest) returns (Message) \{
        option (google.api.http) = \{
            get: &#34;/v1/\{name=messages/*\}&#34;
        \};
      \}
    \}
    message GetMessageRequest \{
      string name = 1; // Mapped to URL path.
    \}
    message Message \{
      string text = 1; // The resource content.
    \}

This enables an HTTP REST to gRPC mapping as below:

- HTTP: `GET /v1/messages/123456`
- gRPC: `GetMessage(name: &#34;messages/123456&#34;)`

Any fields in the request message which are not bound by the path template
automatically become HTTP query parameters if there is no HTTP request body.
For example:

    service Messaging \{
      rpc GetMessage(GetMessageRequest) returns (Message) \{
        option (google.api.http) = \{
            get:&#34;/v1/messages/\{message_id\}&#34;
        \};
      \}
    \}
    message GetMessageRequest \{
      message SubMessage \{
        string subfield = 1;
      \}
      string message_id = 1; // Mapped to URL path.
      int64 revision = 2;    // Mapped to URL query parameter `revision`.
      SubMessage sub = 3;    // Mapped to URL query parameter `sub.subfield`.
    \}

This enables a HTTP JSON to RPC mapping as below:

- HTTP: `GET /v1/messages/123456?revision=2&amp;sub.subfield=foo`
- gRPC: `GetMessage(message_id: &#34;123456&#34; revision: 2 sub:
SubMessage(subfield: &#34;foo&#34;))`

Note that fields which are mapped to URL query parameters must have a
primitive type or a repeated primitive type or a non-repeated message type.
In the case of a repeated type, the parameter can be repeated in the URL
as `...?param=A&amp;param=B`. In the case of a message type, each field of the
message is mapped to a separate parameter, such as
`...?foo.a=A&amp;foo.b=B&amp;foo.c=C`.

For HTTP methods that allow a request body, the `body` field
specifies the mapping. Consider a REST update method on the
message resource collection:

    service Messaging \{
      rpc UpdateMessage(UpdateMessageRequest) returns (Message) \{
        option (google.api.http) = \{
          patch: &#34;/v1/messages/\{message_id\}&#34;
          body: &#34;message&#34;
        \};
      \}
    \}
    message UpdateMessageRequest \{
      string message_id = 1; // mapped to the URL
      Message message = 2;   // mapped to the body
    \}

The following HTTP JSON to RPC mapping is enabled, where the
representation of the JSON in the request body is determined by
protos JSON encoding:

- HTTP: `PATCH /v1/messages/123456 \{ &#34;text&#34;: &#34;Hi!&#34; \}`
- gRPC: `UpdateMessage(message_id: &#34;123456&#34; message \{ text: &#34;Hi!&#34; \})`

The special name `*` can be used in the body mapping to define that
every field not bound by the path template should be mapped to the
request body.  This enables the following alternative definition of
the update method:

    service Messaging \{
      rpc UpdateMessage(Message) returns (Message) \{
        option (google.api.http) = \{
          patch: &#34;/v1/messages/\{message_id\}&#34;
          body: &#34;*&#34;
        \};
      \}
    \}
    message Message \{
      string message_id = 1;
      string text = 2;
    \}


The following HTTP JSON to RPC mapping is enabled:

- HTTP: `PATCH /v1/messages/123456 \{ &#34;text&#34;: &#34;Hi!&#34; \}`
- gRPC: `UpdateMessage(message_id: &#34;123456&#34; text: &#34;Hi!&#34;)`

Note that when using `*` in the body mapping, it is not possible to
have HTTP parameters, as all fields not bound by the path end in
the body. This makes this option more rarely used in practice when
defining REST APIs. The common usage of `*` is in custom methods
which don&#39;t use the URL at all for transferring data.

It is possible to define multiple HTTP methods for one RPC by using
the `additional_bindings` option. Example:

    service Messaging \{
      rpc GetMessage(GetMessageRequest) returns (Message) \{
        option (google.api.http) = \{
          get: &#34;/v1/messages/\{message_id\}&#34;
          additional_bindings \{
            get: &#34;/v1/users/\{user_id\}/messages/\{message_id\}&#34;
          \}
        \};
      \}
    \}
    message GetMessageRequest \{
      string message_id = 1;
      string user_id = 2;
    \}

This enables the following two alternative HTTP JSON to RPC mappings:

- HTTP: `GET /v1/messages/123456`
- gRPC: `GetMessage(message_id: &#34;123456&#34;)`

- HTTP: `GET /v1/users/me/messages/123456`
- gRPC: `GetMessage(user_id: &#34;me&#34; message_id: &#34;123456&#34;)`

Rules for HTTP mapping

1. Leaf request fields (recursive expansion nested messages in the request
   message) are classified into three categories:
   - Fields referred by the path template. They are passed via the URL path.
   - Fields referred by the [HttpRule.body][google.api.HttpRule.body]. They
   are passed via the HTTP
     request body.
   - All other fields are passed via the URL query parameters, and the
     parameter name is the field path in the request message. A repeated
     field can be represented as multiple query parameters under the same
     name.
 2. If [HttpRule.body][google.api.HttpRule.body] is &#34;*&#34;, there is no URL
 query parameter, all fields
    are passed via URL path and HTTP request body.
 3. If [HttpRule.body][google.api.HttpRule.body] is omitted, there is no HTTP
 request body, all
    fields are passed via URL path and URL query parameters.

Path template syntax

    Template = &#34;/&#34; Segments [ Verb ] ;
    Segments = Segment \{ &#34;/&#34; Segment \} ;
    Segment  = &#34;*&#34; | &#34;**&#34; | LITERAL | Variable ;
    Variable = &#34;\{&#34; FieldPath [ &#34;=&#34; Segments ] &#34;\}&#34; ;
    FieldPath = IDENT \{ &#34;.&#34; IDENT \} ;
    Verb     = &#34;:&#34; LITERAL ;

The syntax `*` matches a single URL path segment. The syntax `**` matches
zero or more URL path segments, which must be the last part of the URL path
except the `Verb`.

The syntax `Variable` matches part of the URL path as specified by its
template. A variable template must not contain other variables. If a variable
matches a single path segment, its template may be omitted, e.g. `\{var\}`
is equivalent to `\{var=*\}`.

The syntax `LITERAL` matches literal text in the URL path. If the `LITERAL`
contains any reserved character, such characters should be percent-encoded
before the matching.

If a variable contains exactly one path segment, such as `&#34;\{var\}&#34;` or
`&#34;\{var=*\}&#34;`, when such a variable is expanded into a URL path on the client
side, all characters except `[-_.~0-9a-zA-Z]` are percent-encoded. The
server side does the reverse decoding. Such variables show up in the
[Discovery
Document](https://developers.google.com/discovery/v1/reference/apis) as
`\{var\}`.

If a variable contains multiple path segments, such as `&#34;\{var=foo/*\}&#34;`
or `&#34;\{var=**\}&#34;`, when such a variable is expanded into a URL path on the
client side, all characters except `[-_.~/0-9a-zA-Z]` are percent-encoded.
The server side does the reverse decoding, except &#34;%2F&#34; and &#34;%2f&#34; are left
unchanged. Such variables show up in the
[Discovery
Document](https://developers.google.com/discovery/v1/reference/apis) as
`\{&#43;var\}`.

Using gRPC API Service Configuration

gRPC API Service Configuration (service config) is a configuration language
for configuring a gRPC service to become a user-facing product. The
service config is simply the YAML representation of the `google.api.Service`
proto message.

As an alternative to annotating your proto file, you can configure gRPC
transcoding in your service config YAML files. You do this by specifying a
`HttpRule` that maps the gRPC method to a REST endpoint, achieving the same
effect as the proto annotation. This can be particularly useful if you
have a proto that is reused in multiple services. Note that any transcoding
specified in the service config will override any matching transcoding
configuration in the proto.

The following example selects a gRPC method and applies an `HttpRule` to it:

    http:
      rules:
        - selector: example.v1.Messaging.GetMessage
          get: /v1/messages/\{message_id\}/\{sub.subfield\}

Special notes

When gRPC Transcoding is used to map a gRPC to JSON REST endpoints, the
proto to JSON conversion must follow the [proto3
specification](https://developers.google.com/protocol-buffers/docs/proto3#json).

While the single segment variable follows the semantics of
[RFC 6570](https://tools.ietf.org/html/rfc6570) Section 3.2.2 Simple String
Expansion, the multi segment variable **does not** follow RFC 6570 Section
3.2.3 Reserved Expansion. The reason is that the Reserved Expansion
does not expand special characters like `?` and `#`, which would lead
to invalid URLs. As the result, gRPC Transcoding uses a custom encoding
for multi segment variables.

The path variables **must not** refer to any repeated or mapped field,
because client libraries are not capable of handling such variable expansion.

The path variables **must not** capture the leading &#34;/&#34; character. The reason
is that the most common use case &#34;\{var\}&#34; does not capture the leading &#34;/&#34;
character. For consistency, all path variables must share the same behavior.

Repeated message fields must not be mapped to URL query parameters, because
no client library can support such complicated mapping.

If an API needs to use a JSON array for request or response body, it can map
the request or response body to a repeated field. However, some gRPC
Transcoding implementations may not support this feature.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| selector | string |  | Selects a method to which this rule applies.

Refer to [selector][google.api.DocumentationRule.selector] for syntax details. |
| get | string |  | Maps to HTTP GET. Used for listing and getting information about resources. |
| put | string |  | Maps to HTTP PUT. Used for replacing a resource. |
| post | string |  | Maps to HTTP POST. Used for creating a resource or performing an action. |
| delete | string |  | Maps to HTTP DELETE. Used for deleting a resource. |
| patch | string |  | Maps to HTTP PATCH. Used for updating a resource. |
| custom | [CustomHttpPattern](#google-api-CustomHttpPattern) |  | The custom pattern is used for specifying an HTTP method that is not included in the `pattern` field, such as HEAD, or &#34;*&#34; to leave the HTTP method unspecified for this rule. The wild-card rule is useful for services that provide content to Web (HTML) clients. |
| body | string |  | The name of the request field whose value is mapped to the HTTP request body, or `*` for mapping all request fields not captured by the path pattern to the HTTP body, or omitted for not having any HTTP request body.

NOTE: the referred field must be present at the top-level of the request message type. |
| response_body | string |  | Optional. The name of the response field whose value is mapped to the HTTP response body. When omitted, the entire response message will be used as the HTTP response body.

NOTE: The referred field must be present at the top-level of the response message type. |
| additional_bindings | [HttpRule](#google-api-HttpRule) | repeated | Additional HTTP bindings for the selector. Nested bindings must not contain an `additional_bindings` field themselves (that is, the nesting may only be one level deep). |





 

 

 

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

