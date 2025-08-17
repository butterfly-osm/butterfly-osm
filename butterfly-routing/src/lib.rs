//! Core routing algorithms and graph processing for butterfly-osm

pub mod dijkstra;
pub mod graph;
pub mod profiles;
pub mod spatial;
pub mod dual_core;
pub mod prs_v2;
pub mod prs_v3;
pub mod weight_compression;
pub mod turn_restriction_tables;
pub mod time_routing;
pub mod thread_architecture;
pub mod sharded_caching;
pub mod load_testing;
pub mod prs_v4_simple;

// Re-export main types
pub use profiles::{
    TransportProfile, AccessLevel, HighwayType, WayAccess, AccessTruthTable,
    ProfileMask, MultiProfileMask, EdgeId, MaskingStats, MaskValidationResult,
    ComponentAnalyzer, Component, ComponentId, ComponentType, ComponentStats,
    SpeedWeightCalculator, SpeedConfig, GradePenalties, GradeParams, GradeTelemetry, EdgeWeight, WeightPenalties, QuantizationStats,
    MultiProfileLoader, ProfileLoadingStats, ProfileAccessibilityStats, RouteEchoResponse,
    ProfileRegressionSuite, TestResult, TestType, TestStatus, ForbiddenEdgeReport, PRSReport, PRSTestSummary,
    create_tags
};

pub use spatial::{
    Point2D, BBox, SpatialEdge, SnapIndex, SnapResult, SnapEngine, SnapIndexStats
};

pub use dual_core::{
    NodeId, TimeWeight, TimeEdge, NavEdge, TurnRestriction, RestrictionType,
    GeometryPass, GraphNode, TimeGraph, NavGraph, DualCoreGraph, DualCoreStats
};

pub use dijkstra::{
    RouteResult, ComputationStats, GraphType, DistanceRouter
};

pub use prs_v2::{
    PRSv2TestResult, PRSv2TestType, PRSv2Metrics, PRSv2Config, 
    ProfileRegressionSuiteV2, PRSv2Report, PRSv2Summary
};

pub use weight_compression::{
    CompressedTimeWeight, CompressionConfig, OverflowTable, WeightCompressionSystem,
    CompressionStats, SystemCompressionStats, EDGE_BLOCK_SIZE
};

pub use turn_restriction_tables::{
    JunctionId, TurnMovement, ProfileTurnMatrix, TurnRestrictionShard, TurnRestrictionTableSystem,
    TurnPenalty, TURN_ALLOWED, TURN_FORBIDDEN, TURN_DISCOURAGED, TARGET_WARM_HIT_RATE,
    ProfileMatrixStats, ShardStats, TurnRestrictionSystemStats
};

pub use time_routing::{
    TimeRouteRequest, TimeRouteResponse, RouteQuality, TimeComputationStats,
    TimeBasedRouter, ProfileRouteConfig, TimeRouterStats, route_endpoint
};

pub use prs_v3::{
    PRSv3TestType, PRSv3TestResult, PRSv3Metrics, PRSv3Config, 
    ProfileRegressionSuiteV3, PRSv3Report, PRSv3Summary
};

pub use thread_architecture::{
    NumaNode, ThreadArena, ThreadPoolConfig, NumaThreadPool, LockFreeHotPath,
    ThreadArchitectureSystem, ThreadArenaStats, ThreadPoolStats, LockFreeStats
};

pub use sharded_caching::{
    TurnCacheEntry, GeometryCacheEntry, CacheShard, ShardedTurnCache, ShardedGeometryCache,
    AutoRebalancingCacheManager, CacheShardStats, ShardedCacheStats, RebalancingAction
};

pub use load_testing::{
    LoadTestConfig, RouteComplexityMix, LoadTestOrchestrator, LoadTestReport,
    AxumStreamingHandler, RoutingStreamChunk
};

pub use prs_v4_simple::{
    PRSv4TestType, PRSv4TestResult, PRSv4Metrics, PRSv4Config,
    ProfileRegressionSuiteV4Simple, PRSv4Report, PRSv4Summary
};

/// Core routing engine
#[derive(Default)]
pub struct Router {}

impl Router {
    pub fn new() -> Self {
        Self {}
    }
}
