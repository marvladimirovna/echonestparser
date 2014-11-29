class Config(object):
    KEY = ""

SEARCH_BASE_URL = "http://developer.echonest.com/api/v4/song/search"
ARTIST_BASE_URL = "http://developer.echonest.com/api/v4/artist/profile"

# we need to sleep sometimes, otherwise echonest throttles us.
SLEEP_INTERVAL = 3

import csv
import json
import urllib
import urllib2
import time
import itertools
import sys
import os

class Song(object):
    """
    Source song information. That's what you usually get from last.fm scrobbler
    """
    def __init__(self, artist, track, date):
        self.artist = artist
        self.track = track
        self.date = date

    def __str__(self):
        return "%s - %s - %s" % (self.artist, self.track, self.date)

    def to_row(self):
        return [self.artist, self.date, self.track]


class SongWithMeta(Song):
    """
    Song + additional information. This is some information I cared about.
    """
    def __init__(self, song, duration, bpm, energy, speechiness, valence, acousticness):
        self.song = song
        self.duration = duration
        self.bpm = bpm
        self.energy = energy
        self.speechiness = speechiness
        self.valence = valence
        self.acousticness = acousticness

    def to_row(self):
        """Convert to a python list, so then it can be serialized to CSV row
        """
        return [self.song.artist,
                self.song.date,
                self.song.track,
                self.duration,
                self.bpm,
                self.energy,
                self.speechiness,
                self.valence,
                self.acousticness]

def read_songs(file_name):
    """ Parses scrobble file into a generator of `Song`
    """
    songs = []
    with open(file_name) as f:
        reader = csv.reader(f, delimiter=",")
        for row in reader:
            artist_raw = row[1]
            track_raw = row[2]
            date = row[0]
            artist = artist_raw.replace("Artist: ", "")
            track = track_raw.replace("Track: ", "")
            song = Song(artist, track, date)
            yield song

def get_artist_genres(artist_id):
    """ gets list of genres by artist_id
    """
    # sleep before doing artist
    time.sleep(SLEEP_INTERVAL)

    # artist_id = song_data["artist_id"]
    # artist_data = urllib2.urlopen(search_url).read()

    artist_params = (
        ("api_key", Config.KEY),
        ("id", artist_id),
        ("format", "json"),
        ("bucket", "genre")
    )
    artist_url = ARTIST_BASE_URL + "?" + urllib.urlencode(artist_params)
    artist_result = urllib2.urlopen(artist_url).read()
    artist_json = json.loads(artist_result)
    genres_raw = artist_json["response"]["artist"]["genres"]
    genres = [x["name"] for x in genres_raw]
    return genres

def process_scrobble_file(file_name):
    """Processes scrobble file
    """
    rows = []

    for song in read_songs(file_name):
        params = urllib.urlencode((
            ("api_key", Config.KEY),
            ("artist", song.artist),
            ("title", song.track),
            ("format", "json"),
            ("results", 1),
            ("bucket", "artist_location"),
            ("bucket", "song_type"),
            ("bucket", "audio_summary")
        ))

        search_url = SEARCH_BASE_URL + "?" + params
        print search_url
        result = urllib2.urlopen(search_url).read()
        result_json = json.loads(result)

        if not result_json["response"]["songs"]:
            continue

        song_data = result_json["response"]["songs"][0]
        audio_summary = song_data["audio_summary"]

        # read artist genres
        genres = get_artist_genres(song_data["artist_id"])

        # loudness = audio_summary["loudness"]
        # duration = audio_summary["duration"]
        # speechiness = audio_summary["speechiness"]
        # valence = audio_summary["valence"]
        # acousticness = audio_summary["acousticness"]
        # bpm = audio_summary["tempo"]

        energy = audio_summary["energy"]

        genres_csv = ",".join(genres or [])

        song_row = song.to_row() + [energy, genres_csv]
        
        rows.append(song_row)
        time.sleep(SLEEP_INTERVAL)
        yield song_row


if __name__ == "__main__":
    Config.KEY = sys.argv[1]

    scrobbles_folder = "scrobbles"
    songs = []

    string_out_file_name = "out.csv"

    with open(string_out_file_name, 'wb') as csvfile:
        songs_writer = csv.writer(csvfile)
        for (dirpath, dirnames, filenames) in os.walk(scrobbles_folder):
            for scrobble_file in filenames:
                for song_row in process_scrobble_file(os.path.join(dirpath, scrobble_file)):
                    songs_writer.writerow(song_row)

    print "finished"
