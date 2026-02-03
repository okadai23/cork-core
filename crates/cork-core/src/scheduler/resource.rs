use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::engine::run::{ResourcePoolKind, ResourcePoolSpec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrencyLimits {
    pub cpu_max: u64,
    pub io_max: u64,
    pub per_provider_max: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderLimit {
    pub provider_id: String,
    pub max_concurrency: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRequest {
    pub resource_id: String,
    pub amount: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceAlternative {
    pub resource_id: String,
    pub estimated_duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ResourceReservation {
    allocations: Vec<ResourceAllocation>,
    pub selected_alternative: Option<String>,
}

impl ResourceReservation {
    fn new(allocations: Vec<ResourceAllocation>, selected_alternative: Option<String>) -> Self {
        Self {
            allocations,
            selected_alternative,
        }
    }
}

impl Drop for ResourceReservation {
    fn drop(&mut self) {
        for allocation in self.allocations.drain(..) {
            allocation
                .semaphore
                .add_permits(allocation.permits as usize);
        }
    }
}

#[derive(Debug, Clone)]
struct ResourceAllocation {
    semaphore: Arc<Semaphore>,
    permits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceManagerError {
    InvalidResourceId(String),
    DuplicateResourceId(String),
    CapacityZero(String),
    CapacityTooLarge(String),
    UnknownResourceId(String),
    InvalidAmount { resource_id: String, amount: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResourceId {
    Cpu,
    Io,
    Provider(String),
    Tool(String),
    Machine(String),
}

impl ResourceId {
    fn parse(raw: &str) -> Result<Self, ResourceManagerError> {
        if raw == "cpu" {
            return Ok(ResourceId::Cpu);
        }
        if raw == "io" {
            return Ok(ResourceId::Io);
        }
        let Some((prefix, rest)) = raw.split_once(':') else {
            return Err(ResourceManagerError::InvalidResourceId(raw.to_string()));
        };
        if rest.is_empty() {
            return Err(ResourceManagerError::InvalidResourceId(raw.to_string()));
        }
        match prefix {
            "provider" => Ok(ResourceId::Provider(rest.to_string())),
            "tool" => Ok(ResourceId::Tool(rest.to_string())),
            "machine" => Ok(ResourceId::Machine(rest.to_string())),
            _ => Err(ResourceManagerError::InvalidResourceId(raw.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
struct ResourcePool {
    kind: ResourcePoolKind,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub struct ResourceManager {
    pools: HashMap<String, ResourcePool>,
}

impl ResourceManager {
    pub fn new(
        concurrency: ConcurrencyLimits,
        provider_limits: Vec<ProviderLimit>,
        resource_pools: Vec<ResourcePoolSpec>,
    ) -> Result<Self, ResourceManagerError> {
        let mut pools = HashMap::new();
        let mut seen_ids = HashSet::new();

        Self::insert_pool(
            &mut pools,
            &mut seen_ids,
            "cpu".to_string(),
            ResourcePoolKind::Cumulative,
            concurrency.cpu_max,
        )?;
        Self::insert_pool(
            &mut pools,
            &mut seen_ids,
            "io".to_string(),
            ResourcePoolKind::Cumulative,
            concurrency.io_max,
        )?;

        for provider in provider_limits {
            let id = format!("provider:{}", provider.provider_id);
            let capacity = provider.max_concurrency.min(concurrency.per_provider_max);
            Self::insert_pool(
                &mut pools,
                &mut seen_ids,
                id,
                ResourcePoolKind::Cumulative,
                capacity,
            )?;
        }

        for pool in resource_pools {
            Self::insert_pool(
                &mut pools,
                &mut seen_ids,
                pool.resource_id,
                pool.kind,
                pool.capacity,
            )?;
        }

        Ok(Self { pools })
    }

    pub fn try_reserve(
        &self,
        requests: &[ResourceRequest],
        alternatives: &[ResourceAlternative],
    ) -> Result<Option<ResourceReservation>, ResourceManagerError> {
        if alternatives.is_empty() {
            return self.try_reserve_set(requests, None);
        }

        for alternative in alternatives {
            let mut combined_requests = requests.to_vec();
            combined_requests.push(ResourceRequest {
                resource_id: alternative.resource_id.clone(),
                amount: 1,
            });

            if let Some(reservation) =
                self.try_reserve_set(&combined_requests, Some(alternative.resource_id.clone()))?
            {
                return Ok(Some(reservation));
            }
        }

        Ok(None)
    }

    fn try_reserve_set(
        &self,
        requests: &[ResourceRequest],
        selected_alternative: Option<String>,
    ) -> Result<Option<ResourceReservation>, ResourceManagerError> {
        let mut allocations = Vec::new();

        for request in requests {
            ResourceId::parse(&request.resource_id)?;
            if request.amount == 0 {
                Self::release_allocations(&mut allocations);
                return Err(ResourceManagerError::InvalidAmount {
                    resource_id: request.resource_id.clone(),
                    amount: request.amount,
                });
            }
            let pool = self.pools.get(&request.resource_id).ok_or_else(|| {
                ResourceManagerError::UnknownResourceId(request.resource_id.clone())
            })?;
            if pool.kind == ResourcePoolKind::Exclusive && request.amount != 1 {
                Self::release_allocations(&mut allocations);
                return Err(ResourceManagerError::InvalidAmount {
                    resource_id: request.resource_id.clone(),
                    amount: request.amount,
                });
            }
            let permits = request.amount;
            let permit = match pool.semaphore.try_acquire_many(permits) {
                Ok(permit) => permit,
                Err(_) => {
                    Self::release_allocations(&mut allocations);
                    return Ok(None);
                }
            };
            permit.forget();
            allocations.push(ResourceAllocation {
                semaphore: Arc::clone(&pool.semaphore),
                permits,
            });
        }

        Ok(Some(ResourceReservation::new(
            allocations,
            selected_alternative,
        )))
    }

    fn insert_pool(
        pools: &mut HashMap<String, ResourcePool>,
        seen_ids: &mut HashSet<String>,
        resource_id: String,
        kind: ResourcePoolKind,
        capacity: u64,
    ) -> Result<(), ResourceManagerError> {
        ResourceId::parse(&resource_id)?;
        if !seen_ids.insert(resource_id.clone()) {
            return Err(ResourceManagerError::DuplicateResourceId(resource_id));
        }
        if capacity == 0 {
            return Err(ResourceManagerError::CapacityZero(resource_id));
        }
        let effective_capacity = match kind {
            ResourcePoolKind::Exclusive => 1,
            ResourcePoolKind::Cumulative => capacity,
        };
        let permits = usize::try_from(effective_capacity)
            .map_err(|_| ResourceManagerError::CapacityTooLarge(resource_id.clone()))?;
        pools.insert(
            resource_id,
            ResourcePool {
                kind,
                semaphore: Arc::new(Semaphore::new(permits)),
            },
        );
        Ok(())
    }

    fn release_allocations(allocations: &mut Vec<ResourceAllocation>) {
        for allocation in allocations.drain(..) {
            allocation
                .semaphore
                .add_permits(allocation.permits as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_limits() -> ConcurrencyLimits {
        ConcurrencyLimits {
            cpu_max: 1,
            io_max: 1,
            per_provider_max: 1,
        }
    }

    #[test]
    fn exclusive_pool_enforces_single_holder() {
        let pools = vec![ResourcePoolSpec {
            resource_id: "machine:alpha".to_string(),
            capacity: 2,
            kind: ResourcePoolKind::Exclusive,
            tags: Vec::new(),
        }];
        let manager = ResourceManager::new(default_limits(), Vec::new(), pools)
            .expect("resource manager build");

        let request = ResourceRequest {
            resource_id: "machine:alpha".to_string(),
            amount: 1,
        };

        let reservation = manager
            .try_reserve(std::slice::from_ref(&request), &[])
            .expect("reserve result")
            .expect("first reservation");

        let second = manager
            .try_reserve(std::slice::from_ref(&request), &[])
            .expect("reserve result");
        assert!(second.is_none());

        drop(reservation);

        let third = manager
            .try_reserve(
                &[ResourceRequest {
                    resource_id: "machine:alpha".to_string(),
                    amount: 1,
                }],
                &[],
            )
            .expect("reserve result");
        assert!(third.is_some());
    }

    #[test]
    fn cpu_and_provider_limits_block_parallel_reservations() {
        let pools = vec![];
        let manager = ResourceManager::new(
            default_limits(),
            vec![ProviderLimit {
                provider_id: "openai".to_string(),
                max_concurrency: 1,
            }],
            pools,
        )
        .expect("resource manager build");

        let cpu_request = ResourceRequest {
            resource_id: "cpu".to_string(),
            amount: 1,
        };
        let provider_request = ResourceRequest {
            resource_id: "provider:openai".to_string(),
            amount: 1,
        };

        let reservation = manager
            .try_reserve(&[cpu_request.clone(), provider_request.clone()], &[])
            .expect("reserve result")
            .expect("first reservation");

        let second = manager
            .try_reserve(&[cpu_request, provider_request], &[])
            .expect("reserve result");
        assert!(second.is_none());

        drop(reservation);
    }
}
