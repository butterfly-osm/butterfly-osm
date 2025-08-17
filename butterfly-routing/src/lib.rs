//! Core routing algorithms and graph processing for butterfly-osm

pub mod cch_customization;
pub mod cch_ordering;
pub mod cch_query;
pub mod dijkstra;
pub mod dual_core;
pub mod graph;
pub mod load_testing;
pub mod profiles;
pub mod prs_v2;
pub mod prs_v3;
pub mod prs_v4_simple;
pub mod prs_v5;
pub mod sharded_caching;
pub mod spatial;
pub mod thread_architecture;
pub mod time_routing;
pub mod turn_restriction_tables;
pub mod weight_compression;

// Re-export main types
pub use profiles::{
    create_tags, AccessLevel, AccessTruthTable, Component, ComponentAnalyzer, ComponentId,
    ComponentStats, ComponentType, EdgeId, EdgeWeight, ForbiddenEdgeReport, GradeParams,
    GradePenalties, GradeTelemetry, HighwayType, MaskValidationResult, MaskingStats,
    MultiProfileLoader, MultiProfileMask, PRSReport, PRSTestSummary, ProfileAccessibilityStats,
    ProfileLoadingStats, ProfileMask, ProfileRegressionSuite, QuantizationStats, RouteEchoResponse,
    SpeedConfig, SpeedWeightCalculator, TestResult, TestStatus, TestType, TransportProfile,
    WayAccess, WeightPenalties,
};

pub use spatial::{BBox, Point2D, SnapEngine, SnapIndex, SnapIndexStats, SnapResult, SpatialEdge};

pub use dual_core::{
    DualCoreGraph, DualCoreStats, GeometryPass, GraphNode, NavEdge, NavGraph, NodeId,
    RestrictionType, TimeEdge, TimeGraph, TimeWeight, TurnRestriction,
};

pub use dijkstra::{ComputationStats, DistanceRouter, GraphType, RouteResult};

pub use prs_v2::{
    PRSv2Config, PRSv2Metrics, PRSv2Report, PRSv2Summary, PRSv2TestResult, PRSv2TestType,
    ProfileRegressionSuiteV2,
};

pub use weight_compression::{
    CompressedTimeWeight, CompressionConfig, CompressionStats, OverflowTable,
    SystemCompressionStats, WeightCompressionSystem, EDGE_BLOCK_SIZE,
};

pub use turn_restriction_tables::{
    JunctionId, ProfileMatrixStats, ProfileTurnMatrix, ShardStats, TurnMovement, TurnPenalty,
    TurnRestrictionShard, TurnRestrictionSystemStats, TurnRestrictionTableSystem,
    TARGET_WARM_HIT_RATE, TURN_ALLOWED, TURN_DISCOURAGED, TURN_FORBIDDEN,
};

pub use time_routing::{
    route_endpoint, ProfileRouteConfig, RouteQuality, TimeBasedRouter, TimeComputationStats,
    TimeRouteRequest, TimeRouteResponse, TimeRouterStats,
};

pub use prs_v3::{
    PRSv3Config, PRSv3Metrics, PRSv3Report, PRSv3Summary, PRSv3TestResult, PRSv3TestType,
    ProfileRegressionSuiteV3,
};

pub use thread_architecture::{
    LockFreeHotPath, LockFreeStats, NumaNode, NumaThreadPool, ThreadArchitectureSystem,
    ThreadArena, ThreadArenaStats, ThreadPoolConfig, ThreadPoolStats,
};

pub use sharded_caching::{
    AutoRebalancingCacheManager, CacheShard, CacheShardStats, GeometryCacheEntry,
    RebalancingAction, ShardedCacheStats, ShardedGeometryCache, ShardedTurnCache, TurnCacheEntry,
};

pub use load_testing::{
    AxumStreamingHandler, LoadTestConfig, LoadTestOrchestrator, LoadTestReport, RouteComplexityMix,
    RoutingStreamChunk,
};

pub use prs_v4_simple::{
    PRSv4Config, PRSv4Metrics, PRSv4Report, PRSv4Summary, PRSv4TestResult, PRSv4TestType,
    ProfileRegressionSuiteV4Simple,
};

pub use cch_ordering::{
    CCHNode, CCHNodeId, CCHOrdering, NestedCell, OrderingConfig, OrderingStats, OrderingWatchdog,
    COARSENED_MIN_CELL_SIZE, DEFAULT_MIN_CELL_SIZE, MAX_ORDERING_WALL_TIME,
};

pub use cch_customization::{
    BackwardCSR, CCHCustomization, CCHShortcut, CCHUpwardEdge, CustomizationConfig, CustomizationStats,
    UpwardCSR, MAX_SHORTCUTS_PER_NODE,
};

pub use cch_query::{
    CCHComputationStats, CCHQueryConfig, CCHQueryEngine, CCHQueryResult,
    PerformanceValidationConfig, MAX_SEARCH_NODES,
};

pub use prs_v5::{
    PRSv5Config, PRSv5Metrics, PRSv5Report, PRSv5Summary, PRSv5TestResult, PRSv5TestType,
    ProfileRegressionSuiteV5,
};

/// Core routing engine
#[derive(Default)]
pub struct Router {}

impl Router {
    pub fn new() -> Self {
        Self {}
    }
}
