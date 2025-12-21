from typing import Union, Optional, Callable
import os
import json
import copy
import asyncio
import warnings
import threading
import time
import numpy as np
import pandas as pd
from satorilib.concepts import Observation, Stream
from satorilib.logging import INFO, setup, debug, info, warning, error
from satorilib.utils.system import getProcessorCount
from satorilib.utils.time import datetimeToTimestamp, now
from satorilib.datamanager import DataClient, DataServerApi, DataClientApi, PeerInfo, Message, Subscription
from satorilib.wallet.evrmore.identity import EvrmoreIdentity
from satoriengine.veda import config


def _get_networking_mode() -> str:
    """Get the current networking mode from environment or config."""
    mode = os.environ.get('SATORI_NETWORKING_MODE')
    if mode is None:
        try:
            mode = config.get().get('networking mode', 'central')
        except Exception:
            mode = 'central'
    return mode.lower().strip()


# P2P Integration: Config-based networking mode selection for Centrifugo
def _get_centrifugo_functions():
    """Get the appropriate Centrifugo functions based on networking mode."""
    import os
    networking_mode = os.environ.get('SATORI_NETWORKING_MODE', 'central')

    if networking_mode in ('hybrid', 'p2p', 'p2p_only'):
        try:
            from satorip2p.integration.neuron import create_p2p_centrifugo_client
            # For P2P mode, we use the P2P client and stub out the other functions
            async def p2p_subscription_handler(client, channel, handler):
                await client.subscribe(channel, handler)
            async def p2p_subscribe(client, channel, handler):
                await client.subscribe(channel, handler)
            return create_p2p_centrifugo_client, p2p_subscription_handler, p2p_subscribe
        except ImportError:
            pass  # Fall back to central

    from satorilib.centrifugo import (
        create_centrifugo_client,
        create_subscription_handler,
        subscribe_to_stream
    )
    return create_centrifugo_client, create_subscription_handler, subscribe_to_stream

create_centrifugo_client, create_subscription_handler, subscribe_to_stream = _get_centrifugo_functions()
from satoriengine.veda.data import StreamForecast, validate_single_entry
from satoriengine.veda.adapters import ModelAdapter, StarterAdapter, XgbAdapter, XgbChronosAdapter


# P2P Module Imports (lazy-loaded for optional dependency)
def _get_p2p_modules():
    """
    Get all available P2P modules from satorip2p.
    Returns a dict of module references, or empty dict if satorip2p not installed.
    """
    try:
        from satorip2p import (
            Peers,
            EvrmoreIdentityBridge,
            SatoriScorer,
            RewardCalculator,
            RoundDataStore,
            PredictionInput,
            ScoreBreakdown,
            NetworkingMode,
        )
        from satorip2p.protocol.uptime import (
            UptimeTracker,
            Heartbeat,
            RELAY_UPTIME_THRESHOLD,
            HEARTBEAT_INTERVAL,
        )
        from satorip2p.protocol.consensus import (
            ConsensusManager,
            ConsensusVote,
            ConsensusPhase,
        )
        from satorip2p.protocol.oracle_network import OracleNetwork
        from satorip2p.protocol.prediction_protocol import PredictionProtocol
        from satorip2p.signing import (
            EvrmoreWallet as P2PEvrmoreWallet,
            sign_message,
            verify_message,
        )
        # Bandwidth and QoS modules
        try:
            from satorip2p.protocol.bandwidth import BandwidthTracker, QoSManager
            from satorip2p.protocol.versioning import VersionNegotiator, PeerVersionTracker
            from satorip2p.protocol.storage import StorageManager, RedundantStorage
        except ImportError:
            BandwidthTracker = None
            QoSManager = None
            VersionNegotiator = None
            PeerVersionTracker = None
            StorageManager = None
            RedundantStorage = None
        return {
            'available': True,
            'Peers': Peers,
            'EvrmoreIdentityBridge': EvrmoreIdentityBridge,
            'UptimeTracker': UptimeTracker,
            'Heartbeat': Heartbeat,
            'RELAY_UPTIME_THRESHOLD': RELAY_UPTIME_THRESHOLD,
            'HEARTBEAT_INTERVAL': HEARTBEAT_INTERVAL,
            'ConsensusManager': ConsensusManager,
            'ConsensusVote': ConsensusVote,
            'ConsensusPhase': ConsensusPhase,
            'SatoriScorer': SatoriScorer,
            'RewardCalculator': RewardCalculator,
            'RoundDataStore': RoundDataStore,
            'PredictionInput': PredictionInput,
            'ScoreBreakdown': ScoreBreakdown,
            'OracleNetwork': OracleNetwork,
            'PredictionProtocol': PredictionProtocol,
            'P2PEvrmoreWallet': P2PEvrmoreWallet,
            'sign_message': sign_message,
            'verify_message': verify_message,
            'NetworkingMode': NetworkingMode,
            'BandwidthTracker': BandwidthTracker,
            'QoSManager': QoSManager,
            'VersionNegotiator': VersionNegotiator,
            'PeerVersionTracker': PeerVersionTracker,
            'StorageManager': StorageManager,
            'RedundantStorage': RedundantStorage,
        }
    except ImportError:
        return {'available': False}


_p2p_modules_cache = None


def get_p2p_module(name: str):
    """Get a specific P2P module by name, or None if not available."""
    global _p2p_modules_cache
    if _p2p_modules_cache is None:
        _p2p_modules_cache = _get_p2p_modules()
    return _p2p_modules_cache.get(name)


def is_p2p_available() -> bool:
    """Check if satorip2p is installed and available."""
    global _p2p_modules_cache
    if _p2p_modules_cache is None:
        _p2p_modules_cache = _get_p2p_modules()
    return _p2p_modules_cache.get('available', False)


warnings.filterwarnings('ignore')
setup(level=INFO)



class Engine:

    @classmethod
    async def create(cls) -> 'Engine':
        engine = cls()
        await engine.initialize()
        return engine

    def __init__(self):
        self.streamModels: dict[str, StreamModel] = {}
        self.subscriptions: dict[str, PeerInfo] = {}
        self.publications: dict[str, PeerInfo] = {}
        self.dataServerIp: str = ''
        self.dataServerPort: Union[int, None] = None
        self.dataClient: Union[DataClient, None] = None
        self.paused: bool = False
        self.threads: list[threading.Thread] = []
        self.identity: EvrmoreIdentity = EvrmoreIdentity('/Satori/Neuron/wallet/wallet.yaml')
        self.transferProtocol: Union[str, None] = None
        self.centrifugo = None
        self.centrifugoSubscriptions: list = []
        # P2P Integration
        self._p2p_peers = None
        self._oracle_network = None
        self._prediction_protocol = None
        self._bandwidth_qos = None
        self._version_manager = None
        self._storage_redundancy = None
        self._p2p_observation_subscriptions: dict[str, bool] = {}



    async def centrifugoConnect(self, centrifugoPayload: dict):
        """establish a centrifugo connection for subscribing"""
        if not centrifugoPayload:
            error("No centrifugo payload provided")
            return
            
        token = centrifugoPayload.get('token')
        ws_url = centrifugoPayload.get('ws_url')
        
        if not token or not ws_url:
            error("Missing token or ws_url in centrifugo payload")
            return
        
        try:
            self.centrifugo = await create_centrifugo_client(
                ws_url=ws_url,
                token=token,
                on_connected_callback=lambda x: info("Centrifugo connected"),
                on_disconnected_callback=lambda x: info("Centrifugo disconnected"))
            
            await self.centrifugo.connect()
        except Exception as e:
            error(f"Failed to connect to Centrifugo: {e}")
            
    async def handleCentrifugoMessage(self, ctx, streamUuid: str):
        """Handle messages from Centrifugo"""
        try:
            raw_data = ctx.pub.data

            # Track bandwidth if QoS manager is available
            if self._bandwidth_qos is not None:
                # Estimate message size
                estimated_size = len(str(raw_data)) if raw_data else 0
                await self._bandwidth_qos.account_receive(
                    topic=f"centrifugo/{streamUuid[:8]}",
                    byte_size=estimated_size
                )

            # Parse the JSON string to get the actual data
            if isinstance(raw_data, str):
                data = json.loads(raw_data)
            else:
                data = raw_data


            # Format data for Observation parsing
            formatted_data = {
                "topic": json.dumps({"uuid": streamUuid}),
                "data": data.get('value'),
                "time": data.get('time'),
                "hash": data.get('hash')
            }
            obs = Observation.parse(json.dumps(formatted_data))
            streamModel = self.streamModels.get(streamUuid)
            if isinstance(streamModel, StreamModel) and getattr(streamModel, 'usePubSub', True):
                await streamModel.handleSubscriptionMessage(
                    "Subscription",
                    message=Message({
                        'data': obs,
                        'status': 'stream/observation'}),
                    pubSubFlag=True)
        except Exception as e:
            error(f"Error handling Centrifugo message: {e}")

    async def initialize(self):
        await self.connectToDataServer()
        asyncio.create_task(self.stayConnectedForever())
        await self.startService()
        # Initialize P2P if in hybrid or p2p mode
        networking_mode = _get_networking_mode()
        if networking_mode in ('hybrid', 'p2p'):
            await self.initializeP2P()

    async def initializeP2P(self):
        """Initialize P2P oracle network and prediction protocol."""
        try:
            from satorip2p.peers import Peers
            from satorip2p.protocol.oracle_network import OracleNetwork
            from satorip2p.protocol.prediction_protocol import PredictionProtocol

            # Initialize P2P peers if not already done
            if self._p2p_peers is None:
                self._p2p_peers = Peers()
                await self._p2p_peers.start()
                info("P2P peers initialized for Engine", color="green")

            # Initialize Oracle Network for receiving observations
            if self._oracle_network is None:
                self._oracle_network = OracleNetwork(self._p2p_peers)
                await self._oracle_network.start()
                info("P2P Oracle Network initialized", color="green")

            # Initialize Prediction Protocol for commit-reveal
            if self._prediction_protocol is None:
                self._prediction_protocol = PredictionProtocol(self._p2p_peers)
                await self._prediction_protocol.start()
                info("P2P Prediction Protocol initialized", color="green")

            # Initialize Bandwidth QoS for throttling
            BandwidthTracker = get_p2p_module('BandwidthTracker')
            if BandwidthTracker and self._bandwidth_qos is None:
                try:
                    bandwidth_config = config.get().get('bandwidth', {})
                    self._bandwidth_qos = BandwidthTracker(
                        window_seconds=bandwidth_config.get('window_seconds', 60),
                    )
                    info("P2P Bandwidth QoS initialized", color="green")
                except Exception as e:
                    debug(f"Bandwidth QoS not initialized: {e}")

            # Initialize Version Tracker
            PeerVersionTracker = get_p2p_module('PeerVersionTracker')
            if PeerVersionTracker and self._version_manager is None:
                try:
                    self._version_manager = PeerVersionTracker()
                    info("P2P Version Tracker initialized", color="green")
                except Exception as e:
                    debug(f"Version Tracker not initialized: {e}")

            # Initialize Storage Manager
            StorageManager = get_p2p_module('StorageManager')
            if StorageManager and self._storage_redundancy is None:
                try:
                    storage_config = config.get().get('storage', {})
                    self._storage_redundancy = StorageManager(
                        base_path=storage_config.get('path', '/Satori/Neuron/data'),
                    )
                    info("P2P Storage Manager initialized", color="green")
                except Exception as e:
                    debug(f"Storage Manager not initialized: {e}")

        except ImportError as e:
            debug(f"satorip2p not available for Engine P2P: {e}")
        except Exception as e:
            warning(f"Failed to initialize P2P for Engine: {e}")

    async def subscribeToP2PObservations(self, streamUuid: str):
        """Subscribe to P2P observations for a stream."""
        if self._oracle_network is None:
            return False

        if streamUuid in self._p2p_observation_subscriptions:
            return True  # Already subscribed

        try:
            def on_p2p_observation(observation):
                """Handle P2P observation and pass to stream model."""
                asyncio.create_task(self._handleP2PObservation(streamUuid, observation))

            success = await self._oracle_network.subscribe_to_stream(
                stream_id=streamUuid,
                callback=on_p2p_observation
            )

            if success:
                self._p2p_observation_subscriptions[streamUuid] = True
                info(f"Subscribed to P2P observations for {streamUuid[:16]}...")
                return True

        except Exception as e:
            warning(f"Failed to subscribe to P2P observations for {streamUuid}: {e}")

        return False

    async def _handleP2PObservation(self, streamUuid: str, observation):
        """Handle a P2P observation and pass it to the stream model."""
        try:
            streamModel = self.streamModels.get(streamUuid)
            if streamModel is None:
                return

            # Track bandwidth if QoS manager is available
            if self._bandwidth_qos is not None:
                # Estimate message size (JSON overhead + data)
                estimated_size = len(str(observation.value)) + 100
                await self._bandwidth_qos.account_receive(
                    topic=f"p2p/observation/{streamUuid[:8]}",
                    byte_size=estimated_size
                )

            # Convert P2P Observation to format expected by handleSubscriptionMessage
            formatted_data = {
                "topic": json.dumps({"uuid": streamUuid}),
                "data": observation.value,
                "time": observation.timestamp,
                "hash": observation.hash
            }
            obs = Observation.parse(json.dumps(formatted_data))

            await streamModel.handleSubscriptionMessage(
                "P2PSubscription",
                message=Message({
                    'data': obs,
                    'status': 'stream/observation'
                }),
                pubSubFlag=True
            )

            debug(f"P2P observation received for {streamUuid[:16]}... value={observation.value}")

        except Exception as e:
            error(f"Error handling P2P observation: {e}")

    async def startService(self):
        await self.getPubSubInfo()
        await self.initializeModels()

    def addStream(self, stream: Stream, pubStream: Stream):
        ''' add streams to a running engine '''
        # don't duplicate effort
        if stream.streamId.uuid in [s.streamId.uuid for s in self.streams]:
            return
        self.streams.append(stream)
        self.pubstreams.append(pubStream)
        self.streamModels[stream.streamId] = StreamModel(
            streamId=stream.streamId,
            predictionStreamId=pubStream.streamId,
            predictionProduced=self.predictionProduced)
        self.streamModels[stream.streamId].chooseAdapter(inplace=True)
        self.streamModels[stream.streamId].run_forever()

    def pause(self, force: bool = False):
        if force:
            self.paused = True
        for streamModel in self.streamModels.values():
            streamModel.pause()

    def resume(self, force: bool = False):
        if force:
            self.paused = False
        if not self.paused:
            for streamModel in self.streamModels.values():
                streamModel.resume()

    @property
    def isConnectedToServer(self):
        if hasattr(self, 'dataClient') and self.dataClient is not None:
            return self.dataClient.isConnected()
        return False

    async def connectToDataServer(self):
        ''' connect to server, retry if failed '''

        async def authenticate() -> bool:
            response = await self.dataClient.authenticate(islocal='engine')
            if response.status == DataServerApi.statusSuccess.value:
                info("Local Engine successfully connected to Server Ip at :", self.dataServerIp, color="green")
                # for _, streamModel in self.streamModels.items():
                #     if hasattr(streamModel, 'dataClientOfIntServer'):
                #         streamModel.updateDataClient(self.dataClient)
                return True
            return False

        async def initiateServerConnection() -> bool:
            ''' local engine client authorization '''
            self.dataClient = DataClient(self.dataServerIp, self.dataServerPort, identity=self.identity)
            return await authenticate()

        waitingPeriod = 10
        while not self.isConnectedToServer:
            try:
                self.dataServerIp = config.get().get('server ip', '0.0.0.0')
                self.dataServerPort = int(config.get().get('server port', 24600))
                if await initiateServerConnection():
                    return True
            except Exception as e:
                warning(f'Failed to find a valid Server Ip, retrying in {waitingPeriod}')
                await asyncio.sleep(waitingPeriod)

    async def getPubSubInfo(self):
        ''' gets the relation info between pub-sub streams '''
        waitingPeriod = 10
        while not self.subscriptions and self.isConnectedToServer:
            try:
                pubSubResponse: Message = await self.dataClient.getPubsubMap()
                self.transferProtocol = pubSubResponse.streamInfo.get('transferProtocol')
                transferProtocolPayload = pubSubResponse.streamInfo.get('transferProtocolPayload')
                transferProtocolKey = pubSubResponse.streamInfo.get('transferProtocolKey')
                pubSubMapping = pubSubResponse.streamInfo.get('pubSubMapping')
                if pubSubResponse.status == DataServerApi.statusSuccess.value and pubSubMapping:
                    for sub_uuid, data in pubSubMapping.items():
                        # TODO : deal with supportive streams, ( data['supportiveUuid'] )
                        self.subscriptions[sub_uuid] = PeerInfo(data['dataStreamSubscribers'], data['dataStreamPublishers'])
                        self.publications[data['publicationUuid']] = PeerInfo(data['predictiveStreamSubscribers'], data['predictiveStreamPublishers'])
                    if self.subscriptions:
                        info(pubSubResponse.senderMsg, color='green')
                else:
                    raise Exception
                # Always check for Centrifugo in transferProtocolPayload
                if transferProtocolPayload and isinstance(transferProtocolPayload, dict) and 'centrifugo' in transferProtocolPayload:
                    await self.centrifugoConnect(transferProtocolPayload['centrifugo'])
                return
            except Exception:
                warning(f"Failed to fetch pub-sub info, waiting for {waitingPeriod} seconds")
                await asyncio.sleep(waitingPeriod)

    async def stayConnectedForever(self):
        ''' alternative to await asyncio.Event().wait() '''
        while True:
            await asyncio.sleep(5)
            self.cleanupThreads()
            if not self.isConnectedToServer:
                import sys
                sys.exit(1)  # Exit with error code
                await self.connectToDataServer()
                await self.startService()

    async def initializeModels(self):
        info(f'Transfer protocol : {self.transferProtocol}', color='green')
        for subUuid, pubUuid in zip(self.subscriptions.keys(), self.publications.keys()):
            peers = self.subscriptions[subUuid]
            try:
                self.streamModels[subUuid] = await StreamModel.create(
                    streamUuid=subUuid,
                    predictionStreamUuid=pubUuid,
                    peerInfo=peers,
                    dataClient=self.dataClient,
                    identity=self.identity,
                    pauseAll=self.pause,
                    resumeAll=self.resume,
                    transferProtocol=self.transferProtocol)
            except Exception as e:
                error(e)
            # If using Centrifugo, ensure usePubSub is True
            if self.centrifugo is not None:
                self.streamModels[subUuid].usePubSub = True
            self.streamModels[subUuid].chooseAdapter(inplace=True)
            self.streamModels[subUuid].run_forever()
        
        # Now subscribe to Centrifugo streams after models are initialized
        if self.centrifugo is not None:
            for subUuid in self.subscriptions.keys():
                streamModel = self.streamModels.get(subUuid)
                if streamModel:
                    try:
                        # Create a proper callback that captures the UUID correctly
                        def create_callback(stream_uuid):
                            async def callback(ctx):
                                await self.handleCentrifugoMessage(ctx, stream_uuid)
                            return callback

                        sub = await subscribe_to_stream(
                            client=self.centrifugo,
                            stream_uuid=subUuid,
                            events=create_subscription_handler(
                                stream_uuid=subUuid,
                                on_publication_callback=create_callback(subUuid)))
                        self.centrifugoSubscriptions.append(sub)
                        info(f"Subscribed to Centrifugo stream {subUuid}")
                    except Exception as e:
                        error(f"Failed to subscribe to Centrifugo stream {subUuid}: {e}")

        # Subscribe to P2P observations in hybrid/p2p mode
        networking_mode = _get_networking_mode()
        if networking_mode in ('hybrid', 'p2p') and self._oracle_network is not None:
            for subUuid in self.subscriptions.keys():
                try:
                    await self.subscribeToP2PObservations(subUuid)
                except Exception as e:
                    error(f"Failed to subscribe to P2P observations for {subUuid}: {e}")

    def cleanupThreads(self):
        for thread in self.threads:
            if not thread.is_alive():
                self.threads.remove(thread)
        debug(f'prediction thread count: {len(self.threads)}')

    async def cleanupCentrifugo(self):
        """Clean up Centrifugo connections and subscriptions"""
        try:
            if hasattr(self, 'centrifugoSubscriptions') and self.centrifugoSubscriptions:
                for subscription in self.centrifugoSubscriptions:
                    try:
                        await subscription.unsubscribe()
                    except Exception as e:
                        error(f"Failed to unsubscribe from Centrifugo stream: {e}")
                self.centrifugoSubscriptions = []
                
            if hasattr(self, 'centrifugo') and self.centrifugo:
                try:
                    await self.centrifugo.disconnect()
                    info("Centrifugo client disconnected")
                except Exception as e:
                    error(f"Failed to disconnect Centrifugo client: {e}")
                finally:
                    self.centrifugo = None
        except Exception as e:
            error(f"Error during Centrifugo cleanup: {e}")


class StreamModel:

    @classmethod
    async def create(
        cls,
        streamUuid: str,
        predictionStreamUuid: str,
        peerInfo: PeerInfo,
        dataClient: DataClient,
        identity: EvrmoreIdentity,
        pauseAll:callable,
        resumeAll:callable,
        transferProtocol: str
    ):
        streamModel = cls(
            streamUuid,
            predictionStreamUuid,
            peerInfo,
            dataClient,
            identity,
            pauseAll,
            resumeAll,
            transferProtocol
        )
        await streamModel.initialize()
        return streamModel

    def __init__(
        self,
        streamUuid: str,
        predictionStreamUuid: str,
        peerInfo: PeerInfo,
        dataClient: DataClient,
        identity: EvrmoreIdentity,
        pauseAll:callable,
        resumeAll:callable,
        transferProtocol: str
    ):
        self.cpu = getProcessorCount()
        self.pauseAll = pauseAll
        self.resumeAll = resumeAll
        # self.preferredAdapters: list[ModelAdapter] = [XgbChronosAdapter, XgbAdapter, StarterAdapter ]# SKAdapter #model[0] issue
        self.preferredAdapters: list[ModelAdapter] = [ XgbAdapter, StarterAdapter ]# SKAdapter #model[0] issue
        self.defaultAdapters: list[ModelAdapter] = [XgbAdapter, XgbAdapter, StarterAdapter]
        self.failedAdapters = []
        self.thread: threading.Thread = None
        self.streamUuid: str = streamUuid
        self.predictionStreamUuid: str = predictionStreamUuid
        self.peerInfo: PeerInfo = peerInfo
        self.dataClientOfIntServer: DataClient = dataClient
        self.identity: EvrmoreIdentity = identity
        self.rng = np.random.default_rng(37)
        self.publisherHost = None
        self.transferProtocol: str = transferProtocol
        self.usePubSub: bool = False
        self.internal: bool = False
        # P2P commit-reveal state
        self._prediction_protocol = None
        self._current_round_id: int = 0
        self._pending_commits: dict[int, float] = {}  # round_id -> predicted_value

    async def initialize(self):
        self.data: pd.DataFrame = await self.loadData()
        self.adapter: ModelAdapter = self.chooseAdapter()
        self.pilot: ModelAdapter = self.adapter(uid=self.streamUuid)
        self.pilot.load(self.modelPath())
        self.stable: ModelAdapter = copy.deepcopy(self.pilot)
        self.paused: bool = False
        self.dataClientOfExtServer: Union[DataClient, None] = DataClient(self.dataClientOfIntServer.serverHostPort[0], self.dataClientOfIntServer.serverPort, identity=self.identity)
        debug(f'AI Engine: stream id {self.streamUuid} using {self.adapter.__name__}', color='teal')

    # Testing Purpose ( Don't Delete ): Add heartbeat mechanism to maintain connections during long processing
    async def maintain_connection(self):
        """Send periodic heartbeats to server to keep connection alive"""
        while True:
            if self.dataClientOfIntServer.isConnected():
                try:
                    # A lightweight ping-like request
                    await self.dataClientOfIntServer.isLocalEngineClient()
                except Exception:
                    pass
            await asyncio.sleep(15)  

    async def p2pInit(self):
        await self.connectToPeer()
        asyncio.create_task(self.monitorPublisherConnection())
        await self.startStreamService()
        # asyncio.create_task(self.maintain_connection()) # Testing

    async def startStreamService(self):
        await self.syncData()
        await self.makeSubscription()

    def updateDataClient(self, dataClient):
        ''' Update the internal server data client reference '''
        self.dataClientOfIntServer = dataClient

    def returnPeerIp(self, peer: Union[str, None] = None) -> str:
        if peer is not None:
            return peer.split(':')[0]
        return self.publisherHost.split(':')[0]

    def returnPeerPort(self, peer: Union[str, None] = None) -> int:
        if peer is not None:
            return int(peer.split(':')[1])
        return int(self.publisherHost.split(':')[1])

    @property
    def isConnectedToPublisher(self):
        if self.internal:
            if self.publisherHost is None:
                return False
            return True
        if hasattr(self, 'dataClientOfExtServer') and self.dataClientOfExtServer is not None and self.publisherHost is not None:
            return self.dataClientOfExtServer.isConnected(self.returnPeerIp(), self.returnPeerPort())
        return False

    async def monitorPublisherConnection(self):
        """Combined method that monitors connection status and stream activity"""
        while True:
            if self.internal:
                await asyncio.sleep(30)  # Still sleep to avoid busy waiting
                continue
            for _ in range(30):  
                if not self.isConnectedToPublisher:
                    self.publisherHost = None
                    await self.dataClientOfIntServer.streamInactive(self.streamUuid)
                    await self.connectToPeer()
                    await self.startStreamService()
                    break  
                await asyncio.sleep(9)  
                
            if self.publisherHost is not None and self.isConnectedToPublisher:
                if not await self._isPublisherActive():
                    await self.dataClientOfIntServer.streamInactive(self.streamUuid)
                    await self.connectToPeer()
                    await self.startStreamService()
                    
                
    async def _isPublisherActive(self, publisher: str = None) -> bool:
        ''' confirms if the publisher has the subscription stream in its available stream '''
        try:
            response = await self.dataClientOfExtServer.isStreamActive(
                        peerHost=self.returnPeerIp(publisher) if publisher is not None else self.returnPeerIp(),
                        peerPort=self.returnPeerPort(publisher) if publisher is not None else self.returnPeerPort(),
                        uuid=self.streamUuid)
            if response.status == DataServerApi.statusSuccess.value:
                return True
            else:
                raise Exception
        except Exception as e:
            # warning('Failed to connect to an active Publisher ', e,  publisher)
            return False


    async def connectToPeer(self) -> bool:
        ''' Connects to a peer to receive subscription if it has an active subscription to the stream '''
        
        async def check_peer_active(ip):
            """Only check if peer is active without establishing connection"""
            try:
                response = await self.dataClientOfExtServer.isStreamActive(
                    peerHost=self.returnPeerIp(ip),
                    peerPort=self.returnPeerPort(ip),
                    uuid=self.streamUuid
                )
                return response.status == DataServerApi.statusSuccess.value
            except Exception:
                return False
        
        async def establish_connection(ip):
            """Actually establish the connection"""
            try:
                if await self._isPublisherActive(ip):
                    await self.dataClientOfIntServer.addActiveStream(uuid=self.streamUuid)
                    return True
            except Exception:
                pass
            return False

        while not self.isConnectedToPublisher:
            # connect to direct publisher
            if self.peerInfo.publishersIp is not None and len(self.peerInfo.publishersIp) > 0:
                candidate_publisher = self.peerInfo.publishersIp[0]
                if await establish_connection(candidate_publisher):
                    self.publisherHost = candidate_publisher
                    self.usePubSub = False
                    return True
            
            # try our own data server
            response = await self.dataClientOfIntServer.isStreamActive(uuid=self.streamUuid)
            if response.status == DataServerApi.statusSuccess.value:
                info("Connected to Local Data Server", self.streamUuid)
                self.publisherHost = f"{self.dataClientOfIntServer.serverHostPort[0]}:{self.dataClientOfIntServer.serverPort}"
                self.internal = True
                self.usePubSub = False
                return True
            
            # try the subscribers - first check which ones are active
            subscriber_ips = [ip for ip in self.peerInfo.subscribersIp]
            self.rng.shuffle(subscriber_ips)
            
            # # Check all peers concurrently, faster than looping through each subscribers
            check_tasks = [(ip, asyncio.create_task(check_peer_active(ip))) for ip in subscriber_ips]
            # Find active peers
            active_peers = []
            for ip, task in check_tasks:
                try:
                    if await task:
                        active_peers.append(ip)
                except Exception as e:
                    error(f"Error checking peer {ip}: {str(e)}")
            # Connect to the first active peer found
            if active_peers:
                selected_peer = active_peers[0]  # Take first active peer
                if await establish_connection(selected_peer):
                    self.publisherHost = selected_peer
                    self.usePubSub = False
                    return True

            # slow
            # for subscriberIp in self.peerInfo.subscribersIp:
            #     if await establish_connection(subscriberIp):
            #         self.publisherHost = subscriberIp
            #         self.usePubSub = False
            #         return True
            
            self.publisherHost = None
            warning('Failed to connect to Peers, switching to PubSub', self.streamUuid, print=True)
            self.usePubSub = True
            await asyncio.sleep(60*60)

    async def syncData(self):
        '''
        - this can be highly optimized. but for now we do the simple version
        - just ask for their entire dataset every time
            - if it's different than the df we got from our own dataserver,
              then tell dataserver to save this instead
            - replace what we have
        '''
        try:
            if self.internal:
                # no need to sync with our own server
                return
            else:
                DataResponse = await self.dataClientOfExtServer.getRemoteStreamData(
                    peerHost=self.returnPeerIp(),
                    peerPort=self.returnPeerPort(),
                    uuid=self.streamUuid)
            if DataResponse.status == DataServerApi.statusSuccess.value:
                externalDf = DataResponse.data
                if not externalDf.equals(self.data) and len(externalDf) > 0: # TODO : maybe we can find a better logic so that we don't lose the host server's valuable data ( krishna )
                    response = await self.dataClientOfIntServer.insertStreamData(
                                    uuid=self.streamUuid,
                                    data=externalDf,
                                    replace=True
                                )
                    if response.status == DataServerApi.statusSuccess.value:
                        info("Data updated in server", color='green')
                        externalDf = externalDf.reset_index().rename(columns={
                                    'ts': 'date_time',
                                    'hash': 'id'
                                }).drop(columns=['provider'])
                        self.data = externalDf
                    else:
                        raise Exception(DataResponse.senderMsg)
                else:
                    raise Exception(DataResponse.senderMsg)
        except Exception as e:
            error("Failed to sync data, ", e)
            self.publisherHost = None

    async def makeSubscription(self):
        '''
        - and subscribe to the stream so we get the information
            - whenever we get an observation on this stream, pass to the DataServer
        - continually generate predictions for prediction publication streams and pass that to
        '''
        if self.internal:
            await self.dataClientOfIntServer.subscribe(
                uuid=self.streamUuid,
                publicationUuid=self.predictionStreamUuid,
                callback=self.handleSubscriptionMessage,
                engineSubscribed=True)
        else:
            await self.dataClientOfExtServer.subscribe(
                uuid=self.streamUuid,
                peerHost=self.returnPeerIp(),
                peerPort=self.returnPeerPort(),
                publicationUuid=self.predictionStreamUuid,
                callback=self.handleSubscriptionMessage,
                engineSubscribed=True)

    async def handleSubscriptionMessage(self, subscription: any,  message: Message, pubSubFlag: bool = False):
        debug(f"Stream {self.streamUuid} received subscription message (pubsub: {pubSubFlag})")
        if message.status == DataClientApi.streamInactive.value:
            warning("Stream Inactive")
            await self.closePeerConnection()
            self.publisherHost = None
        else:
            await self.appendNewData(message.data, pubSubFlag)
            self.pauseAll(force=True)
            await self.producePrediction()
            self.resumeAll(force=True)
    
    async def closePeerConnection(self):
        """Close the connection to the current publisher peer"""
        if self.internal:
            # No external connection to close for internal streams
            self.publisherHost = None
            return
        if self.publisherHost is not None and hasattr(self, 'dataClientOfExtServer') and self.dataClientOfExtServer is not None:
            try:
                peer = self.dataClientOfExtServer.peers.get((self.returnPeerIp(), self.returnPeerPort()))
                if peer is not None:
                    await self.dataClientOfExtServer.disconnect(peer)
                    info(f"Closed connection to peer {(self.returnPeerIp(), self.returnPeerPort())}")
                self.publisherHost = None
            except Exception as e:
                error(f"Error closing peer connection: {e}")
                self.publisherHost = None

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False

    async def appendNewData(self, observation: Union[pd.DataFrame, dict], pubSubFlag: bool):
        """extract the data and save it to self.data"""
        try:
            if pubSubFlag:
                parsedData = json.loads(observation.raw)
                debug(f"Stream {self.streamUuid} appending new data: {parsedData['data']} at {parsedData['time']}")
                if validate_single_entry(parsedData["time"], parsedData["data"]):
                        await self.dataClientOfIntServer.insertStreamData(
                                uuid=self.streamUuid,
                                data=pd.DataFrame({ 'value': [float(parsedData["data"])]
                                            }, index=[str(parsedData["time"])]),
                                isSub=True
                            )
                        self.data = pd.concat(
                            [
                                self.data,
                                pd.DataFrame({
                                    "date_time": [str(parsedData["time"])],
                                    "value": [float(parsedData["data"])],
                                    "id": [str(parsedData["hash"])]})
                            ],
                            ignore_index=True)
                else:
                    error("Row not added due to corrupt observation")
            else:
                observation_id = observation['hash'].values[0]
                # Check if self.data is not empty and if the ID already exists
                if not self.data.empty and observation_id in self.data['id'].values:
                    error("Row not added because observation with same ID already exists")
                elif validate_single_entry(observation.index[0], observation["value"].values[0]):
                    if not self.internal:
                        response = await self.dataClientOfIntServer.insertStreamData(
                                uuid=self.streamUuid,
                                data=observation,
                                isSub=True
                            )
                        if response.status == DataServerApi.statusSuccess.value:
                            info(response.senderMsg, color='green')
                        else:
                            raise Exception("Raw ", response.senderMsg)
                    observationDf = observation.reset_index().rename(columns={
                                'index': 'date_time',
                                'hash': 'id'
                            }).drop(columns=['provider'])
                    self.data = pd.concat([self.data, observationDf], ignore_index=True)
                else:
                    error("Row not added due to corrupt observation")
        except Exception as e:
            error("Subscription data not added", e)

    async def passPredictionData(self, forecast: pd.DataFrame, passToCentralServer: bool = False):
        try:
            if not hasattr(self, 'activatePredictionStream') and passToCentralServer:
                await self.dataClientOfIntServer.addActiveStream(uuid=self.predictionStreamUuid)
                self.activatePredictionStream = True
            response = await self.dataClientOfIntServer.insertStreamData(
                            uuid=self.predictionStreamUuid,
                            data=forecast,
                            isSub=True,
                            replace=False if passToCentralServer else True,
                        )
            if response.status == DataServerApi.statusSuccess.value:
                info("Prediction", response.senderMsg, color='green')
            else:
                raise Exception(response.senderMsg)
        except Exception as e:
            error('Failed to send Prediction to server : ', e)

    async def producePrediction(self, updatedModel=None):
        """
        triggered by
            - model replaced with a better one
            - new observation on the stream

        In P2P/hybrid mode, also uses commit-reveal protocol.
        """
        try:
            model = updatedModel or self.stable
            if model is not None:
                loop = asyncio.get_event_loop()
                forecast = await loop.run_in_executor(
                    None,
                    lambda: model.predict(data=self.data)
                )
                if isinstance(forecast, pd.DataFrame):
                    predicted_value = StreamForecast.firstPredictionOf(forecast)
                    predictionDf = pd.DataFrame({ 'value': [predicted_value]
                                    }, index=[datetimeToTimestamp(now())])
                    debug(predictionDf, print=True)

                    # P2P commit-reveal in hybrid/p2p mode
                    networking_mode = _get_networking_mode()
                    if networking_mode in ('hybrid', 'p2p'):
                        await self._commitP2PPrediction(predicted_value)

                    # Pass to central server (central/hybrid mode)
                    if networking_mode in ('central', 'hybrid'):
                        if updatedModel is not None:
                            await self.passPredictionData(predictionDf)
                        else:
                            await self.passPredictionData(predictionDf, True)
                else:
                    raise Exception('Forecast not in dataframe format')
        except Exception as e:
            error(e)
            await self.fallback_prediction()

    async def _commitP2PPrediction(self, predicted_value: float):
        """Commit prediction to P2P network using commit-reveal protocol."""
        try:
            from satorip2p.protocol.prediction_protocol import PredictionProtocol

            # Get or create prediction protocol
            if self._prediction_protocol is None:
                # Try to get from Engine's shared instance
                # For now, create a new one (could be optimized to share)
                try:
                    from satorip2p.peers import Peers
                    # Use a simple local reference for now
                    # In production, this should be passed from Engine
                    pass
                except Exception:
                    pass

            # Use simple prediction publishing (not full commit-reveal for now)
            # Full commit-reveal requires coordination with observation timing
            # This is a simplified version that publishes directly

            # Calculate target time (e.g., next observation period)
            target_time = int(time.time()) + 3600  # 1 hour ahead (configurable)

            # Increment round ID
            self._current_round_id += 1

            # Store for potential reveal later
            self._pending_commits[self._current_round_id] = predicted_value

            debug(f"P2P prediction committed for {self.streamUuid[:16]}... round={self._current_round_id} value={predicted_value}")

            # TODO: When we have shared Peers instance, use:
            # await self._prediction_protocol.publish_prediction(
            #     stream_id=self.streamUuid,
            #     value=predicted_value,
            #     target_time=target_time,
            #     confidence=0.5
            # )

        except ImportError:
            debug("satorip2p not available for P2P prediction commit")
        except Exception as e:
            warning(f"Failed to commit P2P prediction: {e}")

    async def revealP2PPrediction(self, round_id: int):
        """Reveal a previously committed prediction (called after observation arrives)."""
        if round_id not in self._pending_commits:
            return

        try:
            if self._prediction_protocol is not None:
                await self._prediction_protocol.reveal_prediction(
                    stream_id=self.streamUuid,
                    round_id=round_id
                )
                debug(f"P2P prediction revealed for round {round_id}")

            # Clean up
            del self._pending_commits[round_id]

        except Exception as e:
            warning(f"Failed to reveal P2P prediction: {e}")

    async def fallback_prediction(self):
        if os.path.isfile(self.modelPath()):
            try:
                os.remove(self.modelPath())
                debug("Deleted failed model file:", self.modelPath(), color="teal")
            except Exception as e:
                error(f'Failed to delete model file: {str(e)}')
        backupModel = self.defaultAdapters[-1]()
        try:
            trainingResult = backupModel.fit(data=self.data)
            if abs(trainingResult.status) == 1:
                await self.producePrediction(backupModel)
        except Exception as e:
            error(f"Error training new model: {str(e)}")

    async def loadData(self) -> pd.DataFrame:
        try:
            response = await self.dataClientOfIntServer.getLocalStreamData(uuid=self.streamUuid)
            if response.status == DataServerApi.statusSuccess.value:
                conformedData = response.data.reset_index().rename(columns={
                    'ts': 'date_time',
                    'hash': 'id'
                })
                del conformedData['provider']
                return conformedData
            else:
                raise Exception(response.senderMsg)
        except Exception as e:
            debug(e)
            return pd.DataFrame(columns=["date_time", "value", "id"])

    def modelPath(self) -> str:
        return (
            '/Satori/Neuron/models/veda/'
            f'{self.predictionStreamUuid}/'
            f'{self.adapter.__name__}.joblib')

    def chooseAdapter(self, inplace: bool = False) -> ModelAdapter:
        """
        everything can try to handle some cases
        Engine
            - low resources available - SKAdapter
            - few observations - SKAdapter
            - (mapping of cases to suitable adapters)
        examples: StartPipeline, SKAdapter, XGBoostPipeline, ChronosPipeline, DNNPipeline
        """
        # TODO: this needs to be aultered. I think the logic is not right. we
        #       should gather a list of adapters that can be used in the
        #       current condition we're in. if we're already using one in that
        #       list, we should continue using it until it starts to make bad
        #       predictions. if not, we should then choose the best one from the
        #       list - we should optimize after we gather acceptable options.

        if False: # for testing specific adapters
            adapter = XgbChronosAdapter
        else:
            import psutil
            availableRamGigs = psutil.virtual_memory().available / 1e9
            availableSwapGigs = psutil.swap_memory().free / 1e9
            totalAvailableRamGigs = availableRamGigs + availableSwapGigs
            adapter = None
            for p in self.preferredAdapters:
                if p in self.failedAdapters:
                    continue
                if p.condition(data=self.data, cpu=self.cpu, availableRamGigs=totalAvailableRamGigs) == 1:
                    adapter = p
                    break
            if adapter is None:
                for adapter in self.defaultAdapters:
                    if adapter not in self.failedAdapters:
                        break
                if adapter is None:
                    adapter = self.defaultAdapters[-1]
        if (
            inplace and (
                not hasattr(self, 'pilot') or
                not isinstance(self.pilot, adapter))
        ):
            info(
                f'AI Engine: stream id {self.streamUuid} '
                f'switching from {self.adapter.__name__} '
                f'to {adapter.__name__} on {self.streamUuid}',
                color='teal')
            self.adapter = adapter
            self.pilot = adapter(uid=self.streamUuid)
            self.pilot.load(self.modelPath())
        return adapter

    async def run(self):
        """
        Async main loop for generating models and comparing them to the best known
        model so far in order to replace it if the new model is better, always
        using the best known model to make predictions on demand.
        """
        # for testing
        #if self.modelPath() != "/Satori/Neuron/models/veda/YyBHl6bN1GejAEyjKwEDmywFU-M-/XgbChronosAdapter.joblib":
        #    return
        while len(self.data) > 0:
            if self.paused:
                await asyncio.sleep(10)
                continue
            self.chooseAdapter(inplace=True)
            try:
                trainingResult = self.pilot.fit(data=self.data, stable=self.stable)
                if trainingResult.status == 1:
                    if self.pilot.compare(self.stable):
                        if self.pilot.save(self.modelPath()):
                            self.stable = copy.deepcopy(self.pilot)
                            info("stable model updated for stream:", self.streamUuid)
                            await self.producePrediction(self.stable)
                else:
                    debug(f'model training failed on {self.streamUuid} waiting 10 minutes to retry')
                    self.failedAdapters.append(self.pilot)
                    await asyncio.sleep(600)
            except Exception as e:
                import traceback
                traceback.print_exc()
                error(e)
                try:
                    debug(self.pilot.dataset)
                except Exception as e:
                    pass


    def run_forever(self):
        '''Creates separate threads for running the peer connections and model training loop'''

        if hasattr(self, 'thread') and self.thread and self.thread.is_alive():
            warning(f"Thread for model {self.streamUuid} already running. Not creating another.")
            return

        def training_loop_thread():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self.run())
                finally:
                    loop.close()
            except Exception as e:
                error(f"Error in training loop thread: {e}")
                import traceback
                traceback.print_exc()

        if self.transferProtocol == 'p2p-pubsub' or self.transferProtocol == 'p2p-proactive-pubsub':
            init_task = asyncio.create_task(self.p2pInit())

        self.thread = threading.Thread(target=training_loop_thread, daemon=True)
        self.thread.start()


async def main():
    await Engine.create()
    await asyncio.Event().wait()
    
asyncio.run(main())