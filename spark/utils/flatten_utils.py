from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, when, lit

def flatten_competitions(df: DataFrame) -> DataFrame:
    """Select and rename columns from competitions.json."""
    return df.select(
        col("competition_id"),
        col("season_id"),
        col("competition_name"),
        col("competition_gender"),
        col("country_name"),
        col("season_name")
    )

def flatten_matches(df: DataFrame) -> DataFrame:
    """Flatten the nested match object."""
    return df.select(
        col("match_id"),
        col("match_date"),
        col("kick_off"),
        col("home_score"),
        col("away_score"),
        col("match_week"),
        col("competition_stage.id").alias("competition_stage_id"),
        col("competition_stage.name").alias("competition_stage_name"),
        col("competition.id").alias("competition_id"),
        col("season.id").alias("season_id"),
        col("home_team.id").alias("home_team_id"),
        col("home_team.name").alias("home_team_name"),
        col("away_team.id").alias("away_team_id"),
        col("away_team.name").alias("away_team_name"),
        col("stadium.id").alias("stadium_id"),
        col("stadium.name").alias("stadium_name"),
        col("referee.id").alias("referee_id"),
        col("referee.name").alias("referee_name"),
        col("metadata.data_version")
    )

def explode_lineups(df: DataFrame) -> DataFrame:
    """Explode the lineup array to one row per player per team per match."""
    return df.select(
        col("team_id"),
        col("team_name"),
        explode("lineup").alias("player")
    ).select(
        "team_id",
        "team_name",
        col("player.player_id").alias("player_id"),
        col("player.player_name").alias("player_name"),
        col("player.player_nickname").alias("player_nickname"),
        col("player.jersey_number").alias("jersey_number"),
        col("player.country.id").alias("country_id"),
        col("player.country.name").alias("country_name")
    )

def base_events(df: DataFrame) -> DataFrame:
    """Extract common fields from events, keep type‑specific structs."""
    return df.select(
        col("id").alias("event_id"),
        col("index"),
        col("period"),
        col("timestamp"),
        col("minute"),
        col("second"),
        col("type.id").alias("event_type_id"),
        col("type.name").alias("event_type_name"),
        col("possession"),
        col("possession_team.id").alias("possession_team_id"),
        col("possession_team.name").alias("possession_team_name"),
        col("play_pattern.id").alias("play_pattern_id"),
        col("play_pattern.name").alias("play_pattern_name"),
        col("team.id").alias("team_id"),
        col("team.name").alias("team_name"),
        col("player.id").alias("player_id"),
        col("player.name").alias("player_name"),
        col("position.id").alias("position_id"),
        col("position.name").alias("position_name"),
        col("location"),
        col("duration"),
        col("under_pressure"),
        col("off_camera"),
        col("out"),
        col("related_events"),
        # Keep event‑specific structs
        col("pass"),
        col("shot"),
        col("carry"),
        col("duel"),
        col("block"),
        col("dribble"),
        col("clearance"),
        col("interception"),
        col("foul_committed"),
        col("foul_won"),
        col("goalkeeper"),
        col("bad_behaviour"),
        col("substitution"),
        col("tactics")
    )