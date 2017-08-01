CREATE TABLE users (
  id bigint NOT NULL,
  username text NOT NULL,
  gross_profit bigint DEFAULT 0 NOT NULL,
  net_profit bigint DEFAULT 0 NOT NULL,
  games_played bigint DEFAULT 0 NOT NULL,
  -- The timestamp indicates when we have seen the user for the first time.
  created timestamp with time zone DEFAULT now() NOT NULL
);
CREATE SEQUENCE users_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;
ALTER SEQUENCE users_id_seq OWNED BY users.id;


CREATE TABLE licks (
  id bigint NOT NULL,
  user_id bigint NOT NULL,
  message text NOT NULL,
  creator_id bigint NOT NULL,
  created timestamp with time zone DEFAULT now() NOT NULL
);
CREATE SEQUENCE licks_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;
ALTER SEQUENCE licks_id_seq OWNED BY licks.id;


CREATE TYPE UserClassEnum AS ENUM ('user', 'moderator', 'admin');

CREATE TABLE chats (
  id bigint NOT NULL,
  user_id bigint NOT NULL,
  message text NOT NULL,
  created timestamp with time zone DEFAULT now() NOT NULL,
  is_bot boolean NOT NULL,
  channel text NOT NULL
);
CREATE SEQUENCE chats_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;
ALTER SEQUENCE chats_id_seq OWNED BY chats.id;


CREATE TABLE mutes (
  id bigint NOT NULL,
  user_id bigint NOT NULL,
  moderator_id bigint NOT NULL,
  timespec text NOT NULL,
  shadow boolean DEFAULT false NOT NULL,
  created timestamp with time zone DEFAULT now() NOT NULL
);
CREATE SEQUENCE mutes_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;
ALTER SEQUENCE mutes_id_seq OWNED BY mutes.id;


CREATE TABLE unmutes (
  id bigint NOT NULL,
  user_id bigint NOT NULL,
  moderator_id bigint NOT NULL,
  shadow boolean DEFAULT false NOT NULL,
  created timestamp with time zone DEFAULT now() NOT NULL
);
CREATE SEQUENCE unmutes_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;
ALTER SEQUENCE unmutes_id_seq OWNED BY unmutes.id;


CREATE TABLE games (
  id bigint NOT NULL,
  game_crash bigint NOT NULL,
  seed text,
  created timestamp with time zone DEFAULT now() NOT NULL,
  started timestamp with time zone NULL
);
CREATE SEQUENCE games_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;
ALTER SEQUENCE games_id_seq OWNED BY games.id;

CREATE TABLE plays (
  id bigint NOT NULL,
  user_id bigint NOT NULL,
  cash_out bigint,
  game_id bigint NOT NULL,
  bet bigint NOT NULL,
  bonus bigint,
  joined timestamp with time zone NULL
);
CREATE SEQUENCE plays_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;
ALTER SEQUENCE plays_id_seq OWNED BY plays.id;


ALTER TABLE ONLY users ALTER COLUMN id SET DEFAULT nextval('users_id_seq'::regclass);
ALTER TABLE ONLY licks ALTER COLUMN id SET DEFAULT nextval('licks_id_seq'::regclass);
ALTER TABLE ONLY chats ALTER COLUMN id SET DEFAULT nextval('chats_id_seq'::regclass);
ALTER TABLE ONLY mutes ALTER COLUMN id SET DEFAULT nextval('mutes_id_seq'::regclass);
ALTER TABLE ONLY games ALTER COLUMN id SET DEFAULT nextval('games_id_seq'::regclass);
ALTER TABLE ONLY plays ALTER COLUMN id SET DEFAULT nextval('plays_id_seq'::regclass);
ALTER TABLE ONLY unmutes ALTER COLUMN id SET DEFAULT nextval('unmutes_id_seq'::regclass);

ALTER TABLE ONLY users ADD CONSTRAINT users_pkey PRIMARY KEY (id);
ALTER TABLE ONLY licks ADD CONSTRAINT licks_pkey PRIMARY KEY (id);
ALTER TABLE ONLY chats ADD CONSTRAINT chats_pkey PRIMARY KEY (id);
ALTER TABLE ONLY mutes ADD CONSTRAINT mutes_pkey PRIMARY KEY (id);
ALTER TABLE ONLY games ADD CONSTRAINT games_pkey PRIMARY KEY (id);
ALTER TABLE ONLY plays ADD CONSTRAINT plays_pkey PRIMARY KEY (id);
ALTER TABLE ONLY unmutes ADD CONSTRAINT unmutes_pkey PRIMARY KEY (id);

CREATE INDEX licks_user_id_idx ON licks USING btree (user_id);
CREATE INDEX licks_creator_id_idx ON licks USING btree (creator_id);
CREATE INDEX chats_user_id_idx ON chats USING btree (user_id, created DESC);
CREATE INDEX mutes_user_id_idx ON mutes USING btree (user_id);
CREATE INDEX mutes_moderator_id_idx ON mutes USING btree (moderator_id);
CREATE INDEX plays_game_id_idx ON plays USING btree (game_id);
CREATE INDEX plays_user_id_idx ON plays USING btree (user_id, id DESC);
CREATE UNIQUE INDEX unique_username ON users USING btree (lower(username));
CREATE INDEX user_id_idx ON users USING btree (id);
CREATE INDEX unmutes_user_id_idx ON unmutes USING btree (user_id);
CREATE INDEX unmutes_moderator_id_idx ON unmutes USING btree (moderator_id);
CREATE UNIQUE INDEX unique_plays_game_user ON plays USING btree (user_id, game_id);

ALTER TABLE ONLY licks
  ADD CONSTRAINT licks_user_id_fkey
  FOREIGN KEY (user_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY licks
  ADD CONSTRAINT licks_creator_id_fkey
  FOREIGN KEY (creator_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY chats
  ADD CONSTRAINT chats_user_id_fkey
  FOREIGN KEY (user_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY mutes
  ADD CONSTRAINT mutes_user_id_fkey
  FOREIGN KEY (user_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY mutes
  ADD CONSTRAINT mutes_moderator_id_fkey
  FOREIGN KEY (moderator_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY plays
  ADD CONSTRAINT plays_game_id_fkey
  FOREIGN KEY (game_id)
  REFERENCES games(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY plays
  ADD CONSTRAINT plays_user_id_fkey
  FOREIGN KEY (user_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY unmutes
  ADD CONSTRAINT unmutes_user_id_fkey
  FOREIGN KEY (user_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

ALTER TABLE ONLY unmutes
  ADD CONSTRAINT unmutes_moderator_id_fkey
  FOREIGN KEY (moderator_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;


-- Game crash table

-- This is derived data to have fast lookups of game crashes without
-- doing a linear scan through the games table. Fill this table
-- initially with
--   INSERT INTO game_crashes
--     (SELECT game_crash,MAX(id)
--        FROM games
--        GROUP BY game_crash);

CREATE TABLE game_crashes (
  game_crash bigint NOT NULL,
  id bigint NOT NULL
);

ALTER TABLE ONLY game_crashes
  ADD CONSTRAINT game_crashes_pkey
  PRIMARY KEY (game_crash);

ALTER TABLE ONLY game_crashes
  ADD CONSTRAINT game_crashes_id_fkey
  FOREIGN KEY (id)
  REFERENCES games(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

CREATE UNIQUE INDEX game_crashes_crash_idx
  ON game_crashes
  USING btree (game_crash);

CREATE UNIQUE INDEX game_crashes_id_idx
  ON game_crashes
  USING btree (id DESC);


CREATE FUNCTION game_crash_trigger() RETURNS trigger AS
$$
BEGIN
  LOOP
    -- First try to update the key.
    UPDATE game_crashes
      SET id = CASE WHEN NEW.id > id
                 THEN NEW.id
                 ELSE id
               END
      WHERE game_crash = NEW.game_crash;
    IF found THEN RETURN NEW; END IF;
    -- Not there, so try to insert the key. If someone else inserts
    -- the same key concurrently, we could get a unique-key failure.
    BEGIN
      INSERT INTO game_crashes(game_crash,id) VALUES (NEW.game_crash, NEW.id);
      RETURN NEW;
    EXCEPTION WHEN unique_violation THEN
    -- Do nothing, and loop to try the UPDATE again.
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER game_crash_trigger
  AFTER INSERT ON games
  FOR EACH ROW EXECUTE PROCEDURE game_crash_trigger();

CREATE TABLE automutes (
  id bigint NOT NULL,
  creator_id bigint NOT NULL,
  created timestamp with time zone DEFAULT now() NOT NULL,
  regexp text NOT NULL,
  enabled boolean DEFAULT true NOT NULL
);

CREATE SEQUENCE automutes_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;

ALTER SEQUENCE automutes_id_seq
  OWNED BY automutes.id;

ALTER TABLE ONLY automutes
  ALTER COLUMN id
  SET DEFAULT nextval('automutes_id_seq'::regclass);
ALTER TABLE ONLY automutes
  ADD CONSTRAINT automutes_pkey
  PRIMARY KEY (id);
ALTER TABLE ONLY automutes
  ADD CONSTRAINT automutes_creator_id_fkey
  FOREIGN KEY (creator_id)
  REFERENCES users(id)
  ON UPDATE CASCADE
  ON DELETE CASCADE;

CREATE TABLE blocks (
  height integer NOT NULL,
  hash text NOT NULL,
  confirmation timestamp with time zone DEFAULT now() NOT NULL,
  notification timestamp with time zone DEFAULT now() NOT NULL
);
ALTER TABLE ONLY blocks
  ADD CONSTRAINT bv_blocks_pkey
  PRIMARY KEY (height, hash);

CREATE TABLE blocknotifications (
  username text NOT NULL,
  channel_name text NOT NULL
);
ALTER TABLE blocknotifications
  ADD CONSTRAINT bv_blocknotifications_pkey
  PRIMARY KEY (username, channel_name);

CREATE OR REPLACE FUNCTION userstats_trigger()
  RETURNS trigger AS $$

  DECLARE
    delta_user_id bigint;
    delta_gross_profit bigint;
    delta_net_profit bigint;
    delta_games_played bigint;

  -- Work out the increment/decrement amount(s).
  BEGIN
    IF (TG_OP = 'INSERT') THEN
      delta_user_id = NEW.user_id;
      delta_gross_profit = COALESCE(NEW.cash_out-NEW.bet,0::numeric) + COALESCE(NEW.bonus,0::numeric);
      delta_net_profit   = COALESCE(NEW.cash_out, 0::numeric) + COALESCE(NEW.bonus, 0::numeric) - NEW.bet;
      delta_games_played = 1;
    ELSIF (TG_OP = 'DELETE') THEN
      delta_user_id = OLD.user_id;
      delta_gross_profit = - (COALESCE(OLD.cash_out-OLD.bet,0::numeric) + COALESCE(OLD.bonus,0::numeric));
      delta_net_profit   = - (COALESCE(OLD.cash_out, 0::numeric) + COALESCE(OLD.bonus, 0::numeric) - OLD.bet);
      delta_games_played = - 1;
    ELSIF (TG_OP = 'UPDATE') THEN
      IF ( OLD.user_id != NEW.user_id) THEN
        RAISE EXCEPTION
          'Update of user_id : % -> % not allowed', OLD.user_id, NEW.user_id;
      END IF;

      delta_user_id = OLD.user_id;
      delta_gross_profit =
          COALESCE(NEW.cash_out-NEW.bet,0::numeric) + COALESCE(NEW.bonus,0::numeric)
        - (COALESCE(OLD.cash_out-OLD.bet,0::numeric) + COALESCE(OLD.bonus,0::numeric));
      delta_net_profit   =
          COALESCE(NEW.cash_out, 0::numeric) + COALESCE(NEW.bonus, 0::numeric) - NEW.bet
        - (COALESCE(OLD.cash_out, 0::numeric) + COALESCE(OLD.bonus, 0::numeric) - OLD.bet);
      delta_games_played = 0;
    END IF;

    UPDATE users
      SET gross_profit = gross_profit + delta_gross_profit,
          net_profit   = net_profit + delta_net_profit,
          games_played = games_played + delta_games_played
      WHERE id = delta_user_id;

    RETURN NEW;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER userstats_trigger
AFTER INSERT OR UPDATE OR DELETE ON plays
    FOR EACH ROW EXECUTE PROCEDURE userstats_trigger();

CREATE OR REPLACE FUNCTION userIdOf(text) RETURNS bigint AS $$
  SELECT id FROM users WHERE lower(username) = lower($1)
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE VIEW plays_view AS
  SELECT
    game_id,
    g.game_crash,
    g.created,
    username,
    user_id,
    bet,
    cash_out,
    bonus,
    COALESCE(cash_out,0) - bet + COALESCE(bonus,0) AS profit,
    100*cash_out/bet AS factor
FROM games g JOIN plays ON g.id = plays.game_id JOIN users ON user_id = users.id;
