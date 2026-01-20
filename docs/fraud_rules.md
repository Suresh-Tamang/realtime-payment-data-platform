# Fraud Detection Rules - Realtime Payment Data Platform

## Amount Rules

#### AR-004: High Amount Rule
**Description**: Flags transactions with amounts exceeding $10,000 for any merchant.

**Parameters**:
- Amount Threshold: $10,000
- Applies to: All merchants

**Logic**:
```python
.withColumn("rule_high_amount", col("amount") > 10000)
```

**Risk Level**: High
**False Positive Rate**: Low

## Blacklist Rules

#### BL-003: Blacklisted Merchant Rule
**Description**: Flags transactions where the merchant_id appears in a blacklist table.

**Parameters**:
- Blacklist Source: Merchant blacklist DataFrame
- Update Frequency: As needed

**Implementation**:
```python
# Create blacklist DataFrame
blacklist_df = spark.createDataFrame([("M1001",), ("M2002",), ("M9999",)], ["merchant_id"])

# Apply rule using left join
.join(broadcast(blacklist_df.withColumn("rule_blacklist", lit(True))), on="merchant_id", how="left")
.withColumn("rule_blacklist", coalesce(col("rule_blacklist"), lit(False)))
```

**Logic**: Transaction is flagged if merchant_id exists in the blacklist DataFrame.

**Risk Level**: Critical
**False Positive Rate**: Very Low