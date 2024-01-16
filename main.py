import json

import requests
from octopus_api import TentacleSession, OctopusApi

client = OctopusApi(rate=20, resolution="sec")

# channel_ids = input("Space separated list of channel IDs: ").split(" ")
channel_ids = ["UCAWSyEs_Io8MtpY3m-zqILA", "UCahgMxSIQ2zIRrPKhM6Mjvg", "UCGaN6JMiDHZSkPXt-82b6yQ"]


async def get_videos(session: TentacleSession, data: dict):
    async with session.get(url=data['url']) as response:
        return (await response.json())["hits"]["hits"]


async def check_files(session: TentacleSession, data: dict):
    try:
        async with session.head(url=data['url'], timeout=5, raise_for_status=False) as response:
            return {"video_id": data["video_id"], "file": data["file"], "dead": not response.ok, "message": response.reason}
    except TimeoutError as e:
        print(e)
        return {"video_id": data["video_id"], "file": data["file"], "dead": True, "message": "Timed Out (5s)"}


for channel_id in channel_ids:
    max_page = requests.get(
        f"https://archive.ragtag.moe/api/v1/search?channel_id={channel_id}").json()["hits"]["total"]["value"]
    max_page = 10
    print(f"Checking {channel_id} for {max_page} pages of videos")

    request_list = [{
        "url": f"https://archive.ragtag.moe/api/v1/search?channel_id={channel_id}&sort=upload_date&sort_order=desc&from={index}"
        } for index in range(max_page + 1)]
    result = client.execute(requests_list=request_list, func=get_videos)
    videos = [video for sublist in result for video in sublist]

    if not videos:
        break

    file_list = [{
        "url": f"https://content.archive.ragtag.moe/{video['_source']['drive_base']}/{video['_source']['video_id']}/{file['name']}",
        "video_id": video["_source"]["video_id"],
        "file": file["name"]
    }
        for video in videos for file in video["_source"]["files"]]
    checked_files = client.execute(requests_list=file_list, func=check_files)

    video_ids = [video['_source']['video_id'] for video in videos]
    broken_videos = []
    for video_id in video_ids:

        broken_files = []
        files = [file for file in checked_files if file["video_id"] == video_id]
        for file in files:
            if file["dead"]:
                broken_files.append(file)

        if broken_files:
            broken_videos.append({"video_id": video_id, "files": broken_files})

    if broken_videos:
        with(open(f"{channel_id}.json", "w")) as f:
            json.dump(broken_videos, f, indent=4)

        print(f"Checked {channel_id} and written to file, {len(broken_videos)} broken videos")
    else:
        print(f"Checked {channel_id}, no broken videos")
