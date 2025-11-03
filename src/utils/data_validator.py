"""
Data Validation Utilities
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, when, sum as _sum
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate dữ liệu và report issues"""
    
    def __init__(self):
        self.validation_results = {}
    
    def check_missing_values(self, df: DataFrame) -> dict:
        """
        Kiểm tra missing values
        
        Returns:
            Dictionary với tỷ lệ missing cho mỗi column
        """
        logger.info("Checking for missing values...")
        
        total_rows = df.count()
        missing_info = {}
        
        for column in df.columns:
            # Count null and NaN values
            null_count = df.filter(
                col(column).isNull() | isnan(col(column))
            ).count()
            
            missing_pct = (null_count / total_rows) * 100 if total_rows > 0 else 0
            
            missing_info[column] = {
                'missing_count': null_count,
                'missing_percentage': round(missing_pct, 2),
                'total_rows': total_rows
            }
            
            if missing_pct > 0:
                logger.warning(f"Column '{column}': {missing_pct:.2f}% missing values")
        
        self.validation_results['missing_values'] = missing_info
        return missing_info
    
    def check_duplicates(self, df: DataFrame, subset: list = None) -> dict:
        """
        Kiểm tra duplicate rows
        
        Args:
            subset: List of columns to check for duplicates
        """
        logger.info("Checking for duplicates...")
        
        total_rows = df.count()
        
        if subset:
            duplicate_count = df.groupBy(subset).count().filter(col('count') > 1).count()
        else:
            duplicate_count = df.count() - df.distinct().count()
        
        duplicate_pct = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
        
        result = {
            'duplicate_count': duplicate_count,
            'duplicate_percentage': round(duplicate_pct, 2),
            'total_rows': total_rows
        }
        
        if duplicate_pct > 0:
            logger.warning(f"Found {duplicate_count} duplicate rows ({duplicate_pct:.2f}%)")
        else:
            logger.info("✅ No duplicates found")
        
        self.validation_results['duplicates'] = result
        return result
    
    def check_data_types(self, df: DataFrame, expected_schema: dict) -> dict:
        """
        Kiểm tra data types
        
        Args:
            expected_schema: Dict {column_name: expected_type}
        """
        logger.info("Checking data types...")
        
        type_issues = {}
        
        for column, expected_type in expected_schema.items():
            if column in df.columns:
                actual_type = dict(df.dtypes)[column]
                
                if actual_type != expected_type:
                    type_issues[column] = {
                        'expected': expected_type,
                        'actual': actual_type
                    }
                    logger.warning(f"Column '{column}': expected {expected_type}, got {actual_type}")
            else:
                type_issues[column] = {
                    'error': 'Column not found'
                }
                logger.error(f"Column '{column}' not found in DataFrame")
        
        self.validation_results['data_types'] = type_issues
        
        if not type_issues:
            logger.info("✅ All data types match expected schema")
        
        return type_issues
    
    def check_value_ranges(self, df: DataFrame, range_constraints: dict) -> dict:
        """
        Kiểm tra value ranges
        
        Args:
            range_constraints: Dict {column: {'min': value, 'max': value}}
        """
        logger.info("Checking value ranges...")
        
        range_violations = {}
        
        for column, constraints in range_constraints.items():
            if column not in df.columns:
                continue
            
            min_val = constraints.get('min')
            max_val = constraints.get('max')
            
            violations = 0
            
            if min_val is not None:
                violations += df.filter(col(column) < min_val).count()
            
            if max_val is not None:
                violations += df.filter(col(column) > max_val).count()
            
            if violations > 0:
                total = df.count()
                violation_pct = (violations / total) * 100
                
                range_violations[column] = {
                    'violations': violations,
                    'percentage': round(violation_pct, 2),
                    'constraints': constraints
                }
                
                logger.warning(f"Column '{column}': {violations} values outside range {constraints}")
        
        self.validation_results['range_violations'] = range_violations
        
        if not range_violations:
            logger.info("✅ All values within expected ranges")
        
        return range_violations
    
    def check_referential_integrity(self, df1: DataFrame, df2: DataFrame, 
                                   key_column: str) -> dict:
        """
        Kiểm tra referential integrity giữa 2 DataFrames
        """
        logger.info(f"Checking referential integrity on column '{key_column}'...")
        
        # Find orphaned records (records in df1 not in df2)
        orphaned = df1.join(
            df2.select(key_column).distinct(),
            on=key_column,
            how='left_anti'
        ).count()
        
        total = df1.count()
        orphaned_pct = (orphaned / total) * 100 if total > 0 else 0
        
        result = {
            'orphaned_records': orphaned,
            'orphaned_percentage': round(orphaned_pct, 2),
            'total_records': total
        }
        
        if orphaned > 0:
            logger.warning(f"Found {orphaned} orphaned records ({orphaned_pct:.2f}%)")
        else:
            logger.info("✅ Referential integrity intact")
        
        return result
    
    def generate_report(self) -> str:
        """
        Generate validation report
        """
        report = "\n" + "="*80 + "\n"
        report += "DATA VALIDATION REPORT\n"
        report += "="*80 + "\n\n"
        
        for check_name, results in self.validation_results.items():
            report += f"\n{check_name.upper().replace('_', ' ')}:\n"
            report += "-" * 40 + "\n"
            
            if isinstance(results, dict):
                for key, value in results.items():
                    report += f"  {key}: {value}\n"
            else:
                report += f"  {results}\n"
        
        report += "\n" + "="*80 + "\n"
        
        return report
    
    def validate_retail_data(self, df: DataFrame) -> dict:
        """
        Comprehensive validation cho retail dataset
        """
        logger.info("Running comprehensive retail data validation...")
        
        # 1. Check missing values
        self.check_missing_values(df)
        
        # 2. Check duplicates on InvoiceNo + StockCode
        if 'InvoiceNo' in df.columns and 'StockCode' in df.columns:
            self.check_duplicates(df, subset=['InvoiceNo', 'StockCode'])
        
        # 3. Check data types
        expected_schema = {
            'InvoiceNo': 'string',
            'StockCode': 'string',
            'Quantity': 'int',
            'UnitPrice': 'double'
        }
        self.check_data_types(df, expected_schema)
        
        # 4. Check value ranges
        range_constraints = {
            'Quantity': {'min': 0, 'max': 10000},
            'UnitPrice': {'min': 0, 'max': 10000}
        }
        self.check_value_ranges(df, range_constraints)
        
        # Generate and log report
        report = self.generate_report()
        logger.info(report)
        
        return self.validation_results
