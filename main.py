import json

import requests
from octopus_api import TentacleSession, OctopusApi

client = OctopusApi(rate=20, resolution="sec", retries=10, connections=20)

channel_ids = input("Space separated list of channel IDs: ").split(" ")


async def get_videos(session: TentacleSession, data: dict):
    async with session.get(url=data['url']) as response:
        return (await response.json())["hits"]["hits"]


async def check_files(session: TentacleSession, data: dict):
    try:
        async with session.head(url=data['url'], timeout=15, raise_for_status=False) as response:
            if not response.ok:
                return {"video_id": data["video_id"], "file": data["file"], "url": data['url'], "message": response.reason}
    except TimeoutError as e:
        return {"video_id": data["video_id"], "file": data["file"], "url": data['url'], "dead": "unknown", "message": "Timed Out (15s)"}
    except Exception as e:
        return {"video_id": data["video_id"], "file": data["file"], "url": data['url'], "dead": "unknown", "message": f"Unknown Error ({e})"}


for channel_id in channel_ids:
    max_page = requests.get(
        f"https://archive.ragtag.moe/api/v1/search?channel_id={channel_id}").json()["hits"]["total"]["value"]
    print(f"Checking {channel_id} for {max_page} pages of videos")

    request_list = [{
        "url": f"https://archive.ragtag.moe/api/v1/search?channel_id={channel_id}&sort=upload_date&sort_order=desc&from={index}"
    } for index in range(max_page + 1)]
    result = client.execute(requests_list=request_list, func=get_videos)
    videos = [video for sublist in result for video in sublist]

    with open(f"{channel_id}_list.json", "w") as f:
        json.dump(videos, f, indent=4)

    if not videos:
        break

    file_list = [{
        "url": f"https://content.archive.ragtag.moe/{video['_source']['drive_base']}/{video['_source']['video_id']}/{file['name']}",
        "video_id": video["_source"]["video_id"],
        "file": file["name"]
    }
        for video in videos for file in video["_source"]["files"]]
    with open(f"{channel_id}_flist.json", "w") as f:
        json.dump(file_list, f, indent=4)

    checked_files = [file for file in client.execute(requests_list=file_list, func=check_files) if file is not None]

    video_ids = [video['_source']['video_id'] for video in videos]
    with open(f"{channel_id}_cflist.json", "w") as f:
        json.dump(checked_files, f, indent=4)
    broken_videos = []
    for video_id in video_ids:

        broken_files = [json.loads(file) for file in set([json.dumps(file) for file in checked_files if file["video_id"] == video_id])]
        # TODO: Figure out why this is making duplicates
        if broken_files:
            with open(f"{channel_id}_bflist.json", "w") as f:
                json.dump(broken_files, f, indent=4)
            broken_videos.append({"video_id": video_id, "files": broken_files})

    if broken_videos:
        with(open(f"{channel_id}.json", "w")) as f:

            json.dump([json.loads(file) for file in set([json.dumps(file) for file in broken_videos])], f, indent=4)

        print(f"Checked {channel_id} and written to file, {len(broken_videos)} broken videos")
    else:
        print(f"Checked {channel_id}, no broken videos")
