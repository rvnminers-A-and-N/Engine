# SatoriEngine

_The Future Network_

The AI Engine that supports each SatoriNeuron

## Brief Highlevel Overview of the Project

### Highlevel description of code structure

The code describes a few concepts you might want to know about at a high level.

#### Automated Prediction: The Satori AI Engine

The Satori AI Engine is made of mainly two parts: the DataManager and a ModelManager.

A ModelManager builds models that predict the future of one particular datastream. They may consume more than one datastream to help them predict, but their prediction is always aimed at one datastream. They constantly train and retrain models, searching for the best one to predict the future of the datastream. (This essentially comes down to automated feature engineering, selection and hyperparameter tuning).

The DataManager is in charge of getting data for the ModelManager(s). This includes getting updates for all (primary) streams that are predicted by the ModelManager(s) and getting updates on all (secondary) streams they use to help them predict. Aside from getting updates, the DataManager also searches for potentially useful datastreams, downloads their histories, and notifies the ModelManager(s) so they can further evaluate the candidate streams.

#### Simplifying Assumptions

Datastreams should have a simple format and a unique identity. They should have a time column (when the observation took place), and a value column (what the observation was). Using the pub-sub network, or the blockchain, metadata could be saved to describe each datastream. Metadata is not yet primarily relied upon by the system so its format should be a simple json structure. The datetime column could be called 'dt' 'datetime' 'time' or 'date', while the value column should be called 'observation' 'value' or the same as the streams unique identifier. The datetime column should support multiple formats, but UTC time should be preferred in order to easily merge datasets on the correct timing.

ModelManagers produce a prediction of the future, specifically the immediate future, the next timestep. This may sound like a limiting factor, as it seems to be a limit on versatility. However, producing a forecast of multiple observations into the future creates a substantial amount of complexity for the rest of the system. We can push that complexity into a simple structure: have multiple datastreams describing the same data on various timescales: hourly, daily, monthly, etc. With this design, to get a mid- or long-term forecast one merely needs to query multiple predictors.

Speaking of querying predictors, predictors are never literally queried. Instead each predictor (every ModelManager) produces a new datastream of their predictions. In this way their predictions become a freely accessible public good by default, as well as automatically becoming a new datastream other nodes can use in their own models.

## P2P Networking Support

The Engine now supports P2P networking via the `satorip2p` module. When the Neuron is configured for P2P or hybrid networking mode, the Engine automatically uses P2P-enabled Centrifugo connections for real-time data streaming.

### How It Works

The Engine's Centrifugo client import is config-based:
- **Central mode**: Uses standard Centrifugo WebSocket to central servers
- **Hybrid/P2P mode**: Uses P2P GossipSub with central fallback

No code changes are needed - just configure the networking mode in Neuron's config and the Engine adapts automatically.

### Configuration

Set in Neuron's `~/.satori/config/config.yaml`:

```yaml
networking mode: hybrid  # or 'p2p' for pure P2P
```

See the [satorip2p repository](https://github.com/SatoriNetwork/satorip2p) for full documentation.
