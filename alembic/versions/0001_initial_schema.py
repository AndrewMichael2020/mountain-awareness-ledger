from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE EXTENSION IF NOT EXISTS postgis;

        CREATE TABLE events (
          event_id UUID PRIMARY KEY,
          jurisdiction TEXT CHECK (jurisdiction IN ('BC','AB','WA')),
          iso_country TEXT,
          admin_area TEXT,
          location_name TEXT,
          peak_name TEXT,
          route_name TEXT,
          geom GEOGRAPHY(Point, 4326),
          elevation_m INTEGER,
          event_type TEXT CHECK (event_type IN ('fatality')),
          activity TEXT CHECK (activity IN ('alpinism','climbing','hiking','scrambling','ski-mountaineering','unknown')),
          n_fatalities SMALLINT,
          n_injured SMALLINT,
          party_size SMALLINT,
          date_event_start DATE,
          date_event_end DATE,
          date_of_death DATE,
          tz_local TEXT,
          cause_primary TEXT,
          contributing_factors TEXT[],
          weather_context_id UUID,
          avalanche_context_id UUID,
          dedupe_cluster_id UUID,
          extraction_conf NUMERIC(4,3),
          phase TEXT, -- approach, ascent, summit, descent, return (nullable)
          created_at TIMESTAMPTZ DEFAULT now(),
          updated_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE sources (
          source_id UUID PRIMARY KEY,
          event_id UUID REFERENCES events(event_id) ON DELETE CASCADE,
          publisher TEXT,
          article_title TEXT,
          author TEXT,
          url TEXT UNIQUE,
          url_canonical_hash TEXT,
          date_published DATE,
          date_scraped TIMESTAMPTZ,
          paywalled BOOLEAN DEFAULT FALSE,
          license TEXT,
          cleaned_text TEXT,
          summary_bullets TEXT[],
          quoted_evidence JSONB
        );

        CREATE TABLE sar_ops (
          sar_id UUID PRIMARY KEY,
          event_id UUID REFERENCES events(event_id) ON DELETE CASCADE,
          agency TEXT,
          op_type TEXT CHECK (op_type IN ('search','recovery','rescue')),
          started_at TIMESTAMPTZ,
          ended_at TIMESTAMPTZ,
          outcome TEXT,
          notes TEXT
        );

        CREATE TABLE persons_public (
          person_id UUID PRIMARY KEY,
          event_id UUID REFERENCES events(event_id) ON DELETE CASCADE,
          role TEXT CHECK (role IN ('deceased','injured','companion')),
          age SMALLINT,
          sex TEXT,
          hometown TEXT,
          name_redacted BOOLEAN DEFAULT TRUE,
          source_id UUID REFERENCES sources(source_id),
          notes TEXT
        );

        CREATE TABLE enrich_weather (
          weather_context_id UUID PRIMARY KEY,
          provider TEXT,
          ref_time_local TIMESTAMPTZ,
          temp_c NUMERIC(5,2),
          precip_mm NUMERIC(6,2),
          wind_mps NUMERIC(5,2),
          wx_summary TEXT
        );

        CREATE TABLE enrich_avalanche (
          avalanche_context_id UUID PRIMARY KEY,
          provider TEXT,
          danger_rating TEXT,
          problems TEXT[],
          bulletin_url TEXT
        );

        -- People/Org/Relations extension
        CREATE TABLE people (
          person_id UUID PRIMARY KEY,
          full_name TEXT,
          name_public BOOLEAN DEFAULT FALSE,
          sex TEXT,
          age SMALLINT,
          hometown TEXT,
          notes TEXT,
          created_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE person_alias (
          alias_id UUID PRIMARY KEY,
          person_id UUID REFERENCES people(person_id) ON DELETE CASCADE,
          alias TEXT,
          source TEXT,
          UNIQUE(person_id, alias)
        );

        CREATE TABLE organizations (
          org_id UUID PRIMARY KEY,
          org_name TEXT,
          org_type TEXT CHECK (org_type IN ('SAR','Police','Media','Park','NGO','Coroner','Resort','Other')),
          parent_org_id UUID REFERENCES organizations(org_id)
        );

        CREATE TABLE org_unit (
          unit_id UUID PRIMARY KEY,
          org_id UUID REFERENCES organizations(org_id) ON DELETE CASCADE,
          unit_name TEXT
        );

        CREATE TABLE person_affiliation (
          person_id UUID REFERENCES people(person_id) ON DELETE CASCADE,
          org_id UUID REFERENCES organizations(org_id) ON DELETE CASCADE,
          unit_id UUID REFERENCES org_unit(unit_id),
          title TEXT,
          valid_from DATE,
          valid_to DATE,
          PRIMARY KEY (person_id, org_id, valid_from)
        );

        CREATE TABLE person_event_role (
          person_id UUID REFERENCES people(person_id) ON DELETE CASCADE,
          event_id UUID REFERENCES events(event_id) ON DELETE CASCADE,
          role TEXT CHECK (role IN (
            'deceased','injured','companion','partner','guide','witness',
            'rescuer','spokesperson','journalist','photographer','coroner','medical'
          )),
          is_public BOOLEAN DEFAULT TRUE,
          details TEXT,
          PRIMARY KEY (person_id, event_id, role)
        );

        -- Fixed: use surrogate PK + unique indexes for nullable event_id
        CREATE TABLE person_relationship (
          rel_id UUID PRIMARY KEY,
          src_person_id UUID REFERENCES people(person_id) ON DELETE CASCADE,
          dst_person_id UUID REFERENCES people(person_id) ON DELETE CASCADE,
          relation TEXT CHECK (relation IN ('relative','parent','sibling','spouse','friend','partner','teammate','guide_client')),
          event_id UUID REFERENCES events(event_id),
          notes TEXT
        );

        CREATE UNIQUE INDEX ux_person_relationship_null_event
          ON person_relationship (src_person_id, dst_person_id, relation)
          WHERE event_id IS NULL;

        CREATE UNIQUE INDEX ux_person_relationship_with_event
          ON person_relationship (src_person_id, dst_person_id, relation, event_id)
          WHERE event_id IS NOT NULL;

        CREATE TABLE entity_mention (
          mention_id UUID PRIMARY KEY,
          source_id UUID REFERENCES sources(source_id) ON DELETE CASCADE,
          entity_type TEXT CHECK (entity_type IN ('person','organization')),
          entity_id UUID,
          sentence TEXT,
          start_char INT,
          end_char INT,
          quote_type TEXT,
          UNIQUE(source_id, entity_type, entity_id, start_char, end_char)
        );

        CREATE TABLE quotes (
          quote_id UUID PRIMARY KEY,
          source_id UUID REFERENCES sources(source_id) ON DELETE CASCADE,
          person_id UUID REFERENCES people(person_id),
          text TEXT,
          said_at TIMESTAMPTZ,
          context TEXT
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS quotes CASCADE;
        DROP TABLE IF EXISTS entity_mention CASCADE;
        DROP INDEX IF EXISTS ux_person_relationship_with_event;
        DROP INDEX IF EXISTS ux_person_relationship_null_event;
        DROP TABLE IF EXISTS person_relationship CASCADE;
        DROP TABLE IF EXISTS person_event_role CASCADE;
        DROP TABLE IF EXISTS person_affiliation CASCADE;
        DROP TABLE IF EXISTS org_unit CASCADE;
        DROP TABLE IF EXISTS organizations CASCADE;
        DROP TABLE IF EXISTS person_alias CASCADE;
        DROP TABLE IF EXISTS people CASCADE;
        DROP TABLE IF EXISTS enrich_avalanche CASCADE;
        DROP TABLE IF EXISTS enrich_weather CASCADE;
        DROP TABLE IF EXISTS persons_public CASCADE;
        DROP TABLE IF EXISTS sar_ops CASCADE;
        DROP TABLE IF EXISTS sources CASCADE;
        DROP TABLE IF EXISTS events CASCADE;
        """
    )
