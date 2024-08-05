import time
import datetime
import threading
from typing import Literal
from typing import Optional
from pymongo import MongoClient

mongo_client = MongoClient("mongodb://localhost:27017", tz_aware=True)
db = mongo_client.get_database("threadingDemo")
tasks_collection = db.tasks


def create_task(
    id: str,
    name: str,
    status: Literal["scheduled", "sent", "cancel"],
    scheduled_duration_seconds: Optional[int],
):
    return {
        "id": id,
        "name": name,
        "status": status,
        "scheduled_duration_seconds": scheduled_duration_seconds,
    }


def poll_and_send(task_id):
    task = tasks_collection.find_one({"id": task_id})

    if task:
        time.sleep(task["scheduled_duration_seconds"])

        task = tasks_collection.find_one({"id": task_id})

        if task["status"] == "scheduled":
            print("Sending message now...")
            tasks_collection.update_one({"id": task_id}, {"$set": {"status": "sent"}})
            print("Message sent!")

        elif task["status"] == "cancelled":
            print("Task has already been cancelled. Sending operation aborted.")

    return "ok"


if __name__ == "__main__":
    task = create_task(
        id=datetime.datetime.now().strftime("%H:%M:%S"),
        name="Send message",
        status="scheduled",
        scheduled_duration_seconds=3,
    )

    tasks_collection.insert_one(task)

    thread = threading.Thread(target=lambda: poll_and_send(task_id=task["id"]))

    thread.start()

    # tasks_collection.update_one({"id": task["id"]}, {"$set": {"status": "cancelled"}})

    thread.join()
