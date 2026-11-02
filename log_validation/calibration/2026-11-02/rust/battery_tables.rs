// FITTED_R_TABLE: deployed per-cell R (mohm/cell), keyed by cell count.
// Canonical run, split seed=20260623, 35919 real logs, 49 SIM excluded.
const FITTED_R_TABLE: &[(u8, f64)] = &[
    (1, 19.8511),  // n=35260
    (2, 14.5670),  // n=5840
    (3, 8.9016),  // n=16015
    (4, 6.0940),  // n=139149
    (5, 5.4493),  // n=1082
    (6, 3.2710),  // n=446961 LOW-N -> pooled median
    (7, 3.2710),  // n=1355 LOW-N -> pooled median
    (8, 3.2710),  // n=21915 LOW-N -> pooled median
    (9, 3.2710),  // n=7 LOW-N -> pooled median
    (10, 3.2710),  // n=667 LOW-N -> pooled median
    (12, 3.2710),  // n=198042 LOW-N -> pooled median
    (13, 3.2710),  // n=17009 LOW-N -> pooled median
    (14, 3.2710),  // n=14471 LOW-N -> pooled median
];

const FITTED_R_DEFAULT_MOHM_PER_CELL: f64 = 3.2710;  // pooled, n=897773

const OCV_TABLE: &[(f64, f64)] = &[
    (0.0250, 3.5658),
    (0.0750, 3.6218),
    (0.1250, 3.6417),
    (0.1750, 3.6786),
    (0.2250, 3.7638),
    (0.2750, 3.7638),
    (0.3250, 3.7638),
    (0.3750, 3.7932),
    (0.4250, 3.7932),
    (0.4750, 3.7932),
    (0.5250, 3.8045),
    (0.5750, 3.8045),
    (0.6250, 3.8496),
    (0.6750, 3.8990),
    (0.7250, 3.9765),
    (0.7750, 3.9765),
    (0.8250, 3.9991),
    (0.8750, 4.0760),
    (0.9250, 4.0990),
    (0.9750, 4.1545),
];
