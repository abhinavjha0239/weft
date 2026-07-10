// Package enum is the value registry for every SMALLINT code column in the
// schema (see docs/SCHEMA.md: SMALLINT codes instead of PG enums). Values are
// wire/DB contract — append new ones, never renumber.
package enum

// ActorKind attributes an event-log entry (event_log.actor_kind).
type ActorKind int16

const (
	ActorHuman      ActorKind = 1
	ActorAgent      ActorKind = 2
	ActorAutomation ActorKind = 3
	ActorImporter   ActorKind = 4
	ActorSystem     ActorKind = 5
)

// EntityType identifies what an event or reference points at
// (event_log.entity_type, file_reference.entity_type, ...).
type EntityType int16

const (
	EntityMessage   EntityType = 1
	EntityThread    EntityType = 2
	EntityChannel   EntityType = 3
	EntityWorkItem  EntityType = 4
	EntityUser      EntityType = 5
	EntitySpace     EntityType = 6
	EntityFile      EntityType = 7
	EntityOrg       EntityType = 8
	EntityWorkspace EntityType = 9
	EntityGroup     EntityType = 10
	EntityDM        EntityType = 11
	EntityLegalHold EntityType = 12
)

// UserKind is user_account.kind (CC-5: agent covers all non-human principals).
type UserKind int16

const (
	UserHuman               UserKind = 1
	UserAgentPrincipal      UserKind = 2
	UserImportedPlaceholder UserKind = 3
	UserRemote              UserKind = 4
)
