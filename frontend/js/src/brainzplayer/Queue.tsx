import * as React from "react";
import { ReactSortable } from "react-sortablejs";
import NiceModal from "@ebay/nice-modal-react";
import QueueItemCard from "./QueueItemCard";
import ListenCard from "../listens/ListenCard";
import CreateOrEditPlaylistModal from "../playlists/CreateOrEditPlaylistModal";
import { listenToJSPFTrack } from "../playlists/utils";

type QueueProps = {
  queue: BrainzPlayerQueue;
  removeTrackFromQueue: (track: BrainzPlayerQueueItem) => void;
  moveQueueItem: (evt: any) => void;
  setQueue: (queue: BrainzPlayerQueue) => void;
  clearQueue: () => void;
  currentListen?: BrainzPlayerQueueItem;
};

function Queue(props: QueueProps) {
  const {
    queue,
    removeTrackFromQueue,
    setQueue,
    moveQueueItem,
    clearQueue,
    currentListen,
  } = props;

  const [queueNextUp, setQueueNextUp] = React.useState<BrainzPlayerQueue>([]);

  const addQueueToPlaylist = () => {
    const jspfTrackQueue = queue.map((track) => {
      return listenToJSPFTrack(track);
    });

    NiceModal.show(CreateOrEditPlaylistModal, {
      initialTracks: jspfTrackQueue,
    });
  };

  React.useEffect(() => {
    if (currentListen) {
      const currentListenIndex = queue.findIndex(
        (track) => track.id === currentListen.id
      );

      if (
        currentListenIndex === -1 ||
        currentListenIndex === queue.length - 1
      ) {
        setQueueNextUp([]);
      } else {
        const nextUp = queue.slice(currentListenIndex + 1);
        setQueueNextUp(nextUp);
      }
    } else {
      // If there is no current listen track, show all tracks in the queue
      setQueueNextUp([...queue]);
    }
  }, [queue, currentListen]);

  return (
    <div className="queue">
      <h2>Queue</h2>
      {currentListen && (
        <>
          <p className="queue-headers">Now Playing</p>
          <ListenCard
            key="queue-listening-now-card"
            className="queue-item-card"
            listen={currentListen as Listen}
            showTimestamp={false}
            showUsername={false}
          />
        </>
      )}
      <div className="queue-headers">
        <p>Next Up:</p>
        <div className="queue-buttons">
          <button
            className="btn btn-info"
            data-toggle="modal"
            data-target="#CreateOrEditPlaylistModal"
            onClick={addQueueToPlaylist}
            type="button"
          >
            Save to Playlist
          </button>
          <button className="btn btn-info" onClick={clearQueue} type="button">
            Clear Queue
          </button>
        </div>
      </div>
      <div className="queue-list">
        {queueNextUp.length > 0 ? (
          <ReactSortable
            handle=".drag-handle"
            list={queueNextUp}
            onEnd={moveQueueItem}
            setList={() => {}}
          >
            {queueNextUp.map((queueItem: BrainzPlayerQueueItem) => {
              return (
                <QueueItemCard
                  key={queueItem.id}
                  track={queueItem}
                  removeTrackFromQueue={removeTrackFromQueue}
                />
              );
            })}
          </ReactSortable>
        ) : (
          <div className="lead text-center">
            <p>Nothing in this queue yet</p>
          </div>
        )}
      </div>
    </div>
  );
}

export default React.memo(Queue);
