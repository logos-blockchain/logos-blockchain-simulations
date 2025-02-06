// std
use rand::{seq::SliceRandom, Rng};
use std::{collections::HashMap, str::FromStr};
// crates
use serde::{Deserialize, Serialize};
// internal
use crate::{network::behaviour::NetworkBehaviour, node::NodeId};

use super::{NetworkBehaviourKey, NetworkSettings};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum Region {
    NorthAmericaWest,
    NorthAmericaCentral,
    NorthAmericaEast,
    Europe,
    NorthernEurope,
    EastAsia,
    SoutheastAsia,
    Africa,
    SouthAmerica,
    Australia,
}

impl core::fmt::Display for Region {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let s = match self {
            Self::NorthAmericaWest => "NorthAmericaWest",
            Self::NorthAmericaCentral => "NorthAmericaCentral",
            Self::NorthAmericaEast => "NorthAmericaEast",
            Self::Europe => "Europe",
            Self::NorthernEurope => "NorthernEurope",
            Self::EastAsia => "EastAsia",
            Self::SoutheastAsia => "SoutheastAsia",
            Self::Africa => "Africa",
            Self::SouthAmerica => "SouthAmerica",
            Self::Australia => "Australia",
        };
        write!(f, "{s}")
    }
}

impl FromStr for Region {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s
            .trim()
            .to_lowercase()
            .replace(['-', '_', ' '], "")
            .as_str()
        {
            "northamericawest" | "naw" => Ok(Self::NorthAmericaWest),
            "northamericacentral" | "nac" => Ok(Self::NorthAmericaCentral),
            "northamericaeast" | "nae" => Ok(Self::NorthAmericaEast),
            "europe" | "eu" => Ok(Self::Europe),
            "northerneurope" | "neu" => Ok(Self::NorthernEurope),
            "eastasia" | "eas" => Ok(Self::EastAsia),
            "southeastasia" | "seas" => Ok(Self::SoutheastAsia),
            "africa" | "af" => Ok(Self::Africa),
            "southamerica" | "sa" => Ok(Self::SouthAmerica),
            "australia" | "au" => Ok(Self::Australia),
            _ => Err(format!("Unknown region: {s}")),
        }
    }
}

impl Serialize for Region {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let s = match self {
            Self::NorthAmericaWest => "North America West",
            Self::NorthAmericaCentral => "North America Central",
            Self::NorthAmericaEast => "North America East",
            Self::Europe => "Europe",
            Self::NorthernEurope => "Northern Europe",
            Self::EastAsia => "EastAsia",
            Self::SoutheastAsia => "Southeast Asia",
            Self::Africa => "Africa",
            Self::SouthAmerica => "South America",
            Self::Australia => "Australia",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionsData {
    pub regions: HashMap<Region, Vec<NodeId>>,
    #[serde(skip)]
    pub node_region: HashMap<NodeId, Region>,
    pub region_network_behaviour: HashMap<NetworkBehaviourKey, NetworkBehaviour>,
}

impl RegionsData {
    pub fn new(
        regions: HashMap<Region, Vec<NodeId>>,
        region_network_behaviour: HashMap<NetworkBehaviourKey, NetworkBehaviour>,
    ) -> Self {
        let node_region = regions
            .iter()
            .flat_map(|(region, nodes)| nodes.iter().copied().map(|node| (node, *region)))
            .collect();
        Self {
            regions,
            node_region,
            region_network_behaviour,
        }
    }

    pub fn node_region(&self, node_id: NodeId) -> Region {
        self.node_region[&node_id]
    }

    pub fn network_behaviour(&self, node_a: NodeId, node_b: NodeId) -> &NetworkBehaviour {
        let region_a = self.node_region[&node_a];
        let region_b = self.node_region[&node_b];
        self.network_behaviour_between_regions(region_a, region_b)
    }

    pub fn network_behaviour_between_regions(
        &self,
        region_a: Region,
        region_b: Region,
    ) -> &NetworkBehaviour {
        let k = NetworkBehaviourKey::new(region_a, region_b);
        let k_rev = NetworkBehaviourKey::new(region_b, region_a);
        self.region_network_behaviour
            .get(&k)
            .or(self.region_network_behaviour.get(&k_rev))
            .expect("Network behaviour not found for the given regions")
    }

    pub fn region_nodes(&self, region: Region) -> &[NodeId] {
        &self.regions[&region]
    }
}

// Takes a reference to the node_ids and simulation_settings and returns a HashMap
// representing the regions and their associated node IDs.
pub fn create_regions<R: Rng>(
    node_ids: &[NodeId],
    rng: &mut R,
    network_settings: &NetworkSettings,
) -> HashMap<Region, Vec<NodeId>> {
    let mut region_nodes = node_ids.to_vec();
    region_nodes.shuffle(rng);

    let regions = network_settings
        .regions
        .clone()
        .into_iter()
        .collect::<Vec<_>>();

    let last_region_index = regions.len() - 1;

    regions
        .iter()
        .enumerate()
        .map(|(i, (region, distribution))| {
            if i < last_region_index {
                let node_count = (node_ids.len() as f32 * distribution).round() as usize;
                let nodes = region_nodes.drain(..node_count).collect::<Vec<_>>();
                (*region, nodes)
            } else {
                // Assign the remaining nodes to the last region.
                (*region, region_nodes.clone())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::rngs::mock::StepRng;

    use crate::{
        network::{
            regions::{create_regions, Region},
            NetworkSettings,
        },
        node::{NodeId, NodeIdExt},
    };

    #[test]
    fn create_regions_precision() {
        struct TestCase {
            node_count: usize,
            distributions: Vec<f32>,
        }

        let test_cases = vec![
            TestCase {
                node_count: 10,
                distributions: vec![0.5, 0.3, 0.2],
            },
            TestCase {
                node_count: 7,
                distributions: vec![0.6, 0.4],
            },
            TestCase {
                node_count: 20,
                distributions: vec![0.4, 0.3, 0.2, 0.1],
            },
            TestCase {
                node_count: 23,
                distributions: vec![0.4, 0.3, 0.3],
            },
            TestCase {
                node_count: 111,
                distributions: vec![0.3, 0.3, 0.3, 0.1],
            },
            TestCase {
                node_count: 73,
                distributions: vec![0.3, 0.2, 0.2, 0.2, 0.1],
            },
        ];
        let mut rng = StepRng::new(1, 0);

        for tcase in test_cases.iter() {
            let nodes = (0..tcase.node_count)
                .map(NodeId::from_index)
                .collect::<Vec<NodeId>>();

            let available_regions = [
                Region::NorthAmericaWest,
                Region::NorthAmericaCentral,
                Region::NorthAmericaEast,
                Region::Europe,
                Region::NorthernEurope,
                Region::EastAsia,
                Region::SoutheastAsia,
                Region::Africa,
                Region::SouthAmerica,
                Region::Australia,
            ];

            let mut region_distribution = HashMap::new();
            for (region, &dist) in available_regions.iter().zip(&tcase.distributions) {
                region_distribution.insert(*region, dist);
            }

            let settings = NetworkSettings {
                network_behaviors: HashMap::new(),
                regions: region_distribution,
            };

            let regions = create_regions(&nodes, &mut rng, &settings);

            let total_nodes_in_regions = regions.values().map(|v| v.len()).sum::<usize>();
            assert_eq!(total_nodes_in_regions, nodes.len());
        }
    }
}
