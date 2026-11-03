# SuperStore Orders: Data Profile and Agent Implications

This report replaces the earlier `train.csv` profile. The corrected file is **`Data /SuperStoreOrders.csv`**, a 51,290-row global sales extract. The executed analysis is in **`Data-Analytics/main.ipynb`**.

The central point for the agentic AI project is that this is a **line-item table, not an order table and not a customer table**. Most naive group-bys will still run, but several will silently produce wrong answers unless the grain, keys, date parsing, and profit definitions are handled explicitly.

---

## 1. Dataset at a glance

| Item | Value |
|---|---|
| File | `Data /SuperStoreOrders.csv` |
| Rows | 51,290 |
| Original columns | 21 |
| Grain | One product line per row |
| Order dates | 2011-01-01 to 2014-12-31 |
| Ship dates | 2011-01-03 to 2015-01-07 |
| Countries | 147 |
| Markets | 7 |
| Regions | 13 |
| Categories | 3 |
| Sub-categories | 17 |
| Product IDs | 10,292 |
| Customer labels | 795 |
| Distinct `order_id` values | 25,035 |
| Composite order instances | 25,754 |
| Total sales | $12,642,905 |
| Total profit | $1,469,034.82 |
| Aggregate profit margin | 11.62% |

The file is global rather than US-only. It includes `quantity`, `discount`, `profit`, `shipping_cost`, `order_priority`, `market`, and `year`, so the earlier limitations around missing profitability fields no longer apply.

---

## 2. Column dictionary and meaning

| Column | Type | Meaning | Agent note |
|---|---|---|---|
| `order_id` | identifier | Order reference attached to each line | Not unique; 659 IDs span multiple customers |
| `order_date` | date | Date the order was placed | Mixed `/` and `-` formats; parse day-first |
| `ship_date` | date | Date the order shipped | Mixed formats; parse day-first |
| `ship_mode` | categorical | Standard, Second, First, Same Day | Four service levels |
| `customer_name` | label | Customer name | Not a customer ID; heavily reused globally |
| `segment` | categorical | Consumer, Corporate, Home Office | Margins are nearly identical across segments |
| `state` | categorical | State, province, or region | Not globally unique without `country` |
| `country` | categorical | Country | 147 values |
| `market` | categorical | APAC, Africa, Canada, EMEA, EU, LATAM, US | Higher-level geography |
| `region` | categorical | Sub-market region | Nested inside `market` |
| `product_id` | identifier | Product key | Preferred product identifier |
| `category` | categorical | Furniture, Office Supplies, Technology | Three product families |
| `sub_category` | categorical | 17 product sub-families | Tables is the loss-making exception |
| `product_name` | label | Descriptive product name | Not unique as a key |
| `sales` | numeric | Line revenue | Raw CSV uses comma thousands separators |
| `quantity` | numeric | Units on the line | 1 to 14 |
| `discount` | numeric | Line discount rate | 0 to 0.85; median is 0 |
| `profit` | numeric | Line profit | Negative values are common and sometimes extreme |
| `shipping_cost` | numeric | Fulfilment cost | Relationship to `profit` is undocumented |
| `order_priority` | categorical | Low, Medium, High, Critical | Correlates with faster shipping |
| `year` | temporal | Order year | Exactly matches `order_date.year` |

Engineered fields used in the notebook:

- `ship_days = ship_date - order_date`
- `profit_margin = profit / sales`, undefined for the single zero-sales row
- `freight_ratio = shipping_cost / sales`
- `order_key = order_id + customer_name + order_date`
- `discount_band`, `order_month`, `order_quarter`, `order_weekday`

---

## 3. Data quality: the issues that change answers

The source columns have **zero missing values** and there are **zero exact duplicate rows**. The risks are semantic and structural.

### 3.1 `order_id` is not a reliable order key

- 51,290 line items map to only 25,035 distinct `order_id` values.
- 659 order IDs span multiple customers, covering 2,701 rows.
- 58 order IDs span multiple countries.
- 398 span multiple ship modes.
- 510 span multiple states.
- 382 span multiple priorities.

A composite key of **`order_id + customer_name + order_date`** produces 25,754 order instances and removes all cross-country and cross-ship-mode collisions. Example: `IT-2014-1036058` contains 8 rows, 4 customers, 3 states, and 3 categories. Grouping by `order_id` alone would merge unrelated purchases.

### 3.2 `customer_name` is not a customer entity

- There are only 795 customer labels for 51,290 lines and 147 countries.
- Every one of those 795 names appears in more than one country.
- One name appears in 32 countries.
- One name covers up to 47 distinct `order_id` values.

There is no `customer_id` in the file. Customer counts, customer lifetime value, and customer-level retention are not supported without an external entity-resolution key.

### 3.3 Dates require day-first parsing

The raw file mixes slash and dash formats:

- 20,067 slash-formatted `order_date` values and 31,223 dash-formatted values.
- 19,834 slash-formatted `ship_date` values and 31,456 dash-formatted values.
- Examples include `1/1/2011`, `6/1/2011`, and `13-01-2011`.

Day-first parsing gives:

- 0 parse failures.
- 0 negative shipping gaps.
- 0 shipping gaps above 30 days.
- A maximum shipping gap of 7 days.

Month-first parsing of the same strings produces **9,687 negative shipping gaps** and **14,324 gaps over 30 days**. The day-first rule is therefore not cosmetic; it is required for valid fulfilment and cohort analysis.

### 3.4 Sales uses comma thousands separators

- 2,630 raw `sales` values contain commas, for example `"1,648"` and `"3,029"`.
- Loading without `thousands=","` leaves `sales` as text or drops those values from numeric summaries.
- Load with `pd.read_csv(..., thousands=",")` or clean the field before aggregation.

### 3.5 Other key caveats

| Check | Result | Risk |
|---|---|---|
| Missing values in 21 source columns | 0 | Low |
| Missing values in engineered ratios | 2 | Low; caused by the zero-sales line |
| Exact duplicate rows | 0 | Low |
| `order_id` spanning multiple customers | 659 | High for order metrics |
| `customer_name` distinct values | 795 | High for customer metrics |
| Maximum countries for one customer name | 32 | High for customer metrics |
| Duplicate `order_id + product_id` rows | 38 | Low to Medium |
| Zero-sales lines | 1 | Low |
| `profit < -sales` | 2,260 | High for averages |
| `profit < -2 * sales` | 378 | High for averages |
| `shipping_cost > sales` | 1 | Low |
| State names appearing in multiple countries | 16 | Medium |
| Product names mapping to multiple product IDs | 1,944 | Medium |
| Currency column present | No | Medium for cross-country money |
| `year` mismatching `order_date` | 0 | Low |

Use **`product_id`**, not `product_name`, as the product key. Use **`country + state`**, not `state` alone, as the geography key. Treat all money as one common currency only after explicitly stating that assumption.

---

## 4. Numeric profile

| Column | Mean | Median | Mode | Std. dev. | Min | Max | Skew |
|---|---:|---:|---:|---:|---:|---:|---:|
| `sales` | 246.50 | 85.00 | 13.00 | 487.57 | 0.00 | 22,638.00 | 8.14 |
| `quantity` | 3.48 | 3.00 | 2.00 | 2.28 | 1.00 | 14.00 | 1.36 |
| `discount` | 0.1429 | 0.0000 | 0.0000 | 0.2123 | 0.00 | 0.85 | 1.39 |
| `profit` | 28.64 | 9.24 | 0.00 | 174.42 | -6,599.98 | 8,399.98 | 4.16 |
| `shipping_cost` | 26.38 | 7.79 | 0.86 | 57.30 | 0.00 | 933.57 | 5.86 |
| `ship_days` | 3.97 | 4.00 | 4.00 | 1.73 | 0.00 | 7.00 | — |

Key distribution facts:

- `sales` is strongly right-skewed. The median line is $85, the 90th percentile is $632, the 99th percentile is $2,301, and the maximum is $22,638.
- `profit` is even more dispersed. The median line earns $9.24, the 95th percentile is $211.50, and the minimum is -$6,599.98.
- `profit_margin` has a median of 16.93%, a 95th percentile of 47.04%, and a minimum of -472.78%.
- `discount` is zero on 29,009 lines, or 56.56% of the file.
- `shipping_cost` averages 10.70% of sales at the aggregate level and is highly correlated with sales (`r = 0.768`).

---

## 5. Categorical profile

| Column | Distinct values | Most common value | Share of rows |
|---|---:|---|---:|
| `ship_mode` | 4 | Standard Class | 60.00% |
| `segment` | 3 | Consumer | 51.70% |
| `market` | 7 | APAC | 21.45% |
| `region` | 13 | Central | 21.67% |
| `category` | 3 | Office Supplies | 60.97% |
| `sub_category` | 17 | Binders | 11.99% |
| `order_priority` | 4 | Medium | 57.39% |
| `year` | 4 | 2014 | 34.18% |

Two volume patterns matter:

- **Office Supplies has 60.97% of lines but only 29.96% of sales.** It is a high-volume, low-ticket category. Technology has 19.77% of lines but 37.53% of sales.
- **2014 contains 34.18% of rows and 34.01% of sales.** The file is a growth series, not a stationary cross-section.

---

## 6. Dates and fulfilment

| Metric | Value |
|---|---|
| Order date range | 2011-01-01 to 2014-12-31 |
| Ship date range | 2011-01-03 to 2015-01-07 |
| Unique order dates | 1,430 |
| Unique ship dates | 1,464 |
| Median order date | 2013-07-08 |
| Median ship date | 2013-07-12 |
| Minimum shipping gap | 0 days |
| Median shipping gap | 4 days |
| Maximum shipping gap | 7 days |

Shipping service levels behave as expected:

| Ship mode | Rows | Mean ship days | Sales share | Margin | Freight ratio |
|---|---:|---:|---:|---:|---:|
| Standard Class | 30,775 | 5.00 | 59.95% | 11.75% | 8.11% |
| Second Class | 10,309 | 3.23 | 20.29% | 11.46% | 12.24% |
| First Class | 7,505 | 2.18 | 14.48% | 11.37% | 16.83% |
| Same Day | 2,701 | 0.04 | 5.28% | 11.42% | 17.38% |

Order priority also tracks speed:

| Priority | Rows | Mean ship days | Sales share | Margin | Freight ratio |
|---|---:|---:|---:|---:|---:|
| Critical | 3,932 | 1.81 | 7.80% | 12.76% | 23.81% |
| High | 15,501 | 3.09 | 30.12% | 11.04% | 13.38% |
| Medium | 29,433 | 4.52 | 57.59% | 11.87% | 7.46% |
| Low | 2,424 | 6.48 | 4.49% | 10.33% | 11.56% |

### Profit and shipping are separate concepts

Profit margin is almost flat across ship modes (11.37%-11.75%), while the shipping-cost ratio more than doubles from 8.11% for Standard Class to 17.38% for Same Day. That pattern suggests **`profit` is not net of `shipping_cost`**, but the file does not document the definition.

If shipping cost were subtracted mechanically, the profit-minus-shipping margin would be:

- Standard Class: 3.64%
- Second Class: -0.78%
- First Class: -5.46%
- Same Day: -5.97%
- Overall: 0.92%

The agent must not silently switch between gross profit, net profit, and profit-after-shipping. The metric definition should be explicit in every answer.

---

## 7. Economics: sales, profit, discounts, losses

### 7.1 Profit bridge

| Metric | Value |
|---|---:|
| Sales | $12,642,905.00 |
| Profit | $1,469,034.82 |
| Profit margin | 11.62% |
| Loss-making lines | 12,543 |
| Loss-making share | 24.46% |
| Total loss | -$920,357.39 |
| Profitable lines | 38,079 |
| Total profit from profitable lines | $2,389,392.21 |
| Zero-profit lines | 668 |

The aggregate is positive because profitable lines are much larger than loss-making lines. A 24.46% line-level loss rate is still high and should be surfaced in any profitability answer.

### 7.2 Discount is the dominant profit lever

| Discount band | Lines | Sales share | Profit margin | Loss share | Average discount |
|---|---:|---:|---:|---:|---:|
| 0% | 29,009 | 55.31% | 25.32% | 0.00% | 0.00% |
| 0-10% | 4,679 | 15.52% | 17.31% | 19.23% | 8.94% |
| 10-20% | 6,274 | 13.90% | 9.86% | 23.32% | 19.22% |
| 20-30% | 967 | 3.03% | -5.53% | 62.15% | 27.36% |
| 30-40% | 3,400 | 5.55% | -23.68% | 80.21% | 39.69% |
| 40-50% | 1,156 | 1.19% | -37.33% | 92.04% | 45.82% |
| 50-60% | 1,655 | 2.68% | -49.64% | 98.97% | 50.08% |
| 60%+ | 4,150 | 2.83% | -112.86% | 100.00% | 65.86% |

The profit breakpoint is around a 20% discount. From 20%-30% onward, the average line is loss-making, and at 50%+ almost every line loses money.

- Zero-discount lines are never loss-making.
- 5,805 lines have a discount of 50% or more; 99.71% of them lose money.
- Discount and profit margin have a correlation of **-0.846**.
- Discount and profit have a correlation of **-0.316**.

The correlation is descriptive, not causal. The synthetic discount assignment may be confounded with product, country, or campaign effects.

### 7.3 Sensitivity and anomalies

| Scenario | Rows | Sales | Profit | Margin | Loss share |
|---|---:|---:|---:|---:|---:|
| All lines | 51,290 | $12,642,905 | $1,469,034.82 | 11.62% | 24.46% |
| Exclude `profit < -sales` | 49,030 | $12,443,871 | $1,765,882.33 | 14.19% | 20.97% |
| Exclude `discount >= 50%` | 45,485 | $11,947,136 | $2,040,246.36 | 17.08% | 14.85% |
| Both filters | 45,485 | $11,947,136 | $2,040,246.36 | 17.08% | 14.85% |

The two filters produce the same result because **all 2,260 lines with `profit < -sales` are in the 50%+ discount group**. These lines are not explainable by ordinary discount arithmetic:

- 2,260 lines have profit below negative sales.
- 378 have profit below twice negative sales.
- 2,259 have margins below -100%.
- The worst line is `IT-2012-5588535`: $242 sales, 85% discount, -$1,144.12 profit, -472.78% margin.

Treat these as an anomaly/return bucket. Do not let them drive average profit, average margin, or country rankings without a sensitivity check.

---

## 8. Business dimensions

### 8.1 Category

| Category | Line share | Sales | Sales share | Profit | Profit share | Margin | Loss share | Avg. discount |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Technology | 19.77% | $4,744,691 | 37.53% | $663,778.73 | 45.18% | 13.99% | 23.90% | 13.53% |
| Furniture | 19.26% | $4,110,884 | 32.52% | $286,782.25 | 19.52% | 6.98% | 31.55% | 16.81% |
| Office Supplies | 60.97% | $3,787,330 | 29.96% | $518,473.83 | 35.29% | 13.69% | 22.39% | 13.74% |

Furniture is the weak category: it has 32.52% of sales but only 19.52% of profit. Tables is the main reason.

### 8.2 Sub-category

| Sub-category | Sales | Margin | Loss share | Avg. discount |
|---|---:|---:|---:|---:|
| Tables | $757,034 | -8.47% | 57.61% | 29.07% |
| Machines | $779,071 | 7.56% | 30.55% | 16.96% |
| Supplies | $243,090 | 9.29% | 24.29% | 12.79% |
| Chairs | $1,501,682 | 9.45% | 32.00% | 16.31% |
| Storage | $1,127,124 | 9.62% | 27.28% | 13.85% |
| Bookcases | $1,466,559 | 11.04% | 29.74% | 15.38% |
| Furnishings | $385,609 | 12.18% | 25.36% | 15.11% |
| Phones | $1,706,874 | 12.70% | 23.50% | 14.58% |
| Appliances | $1,011,081 | 14.01% | 21.77% | 14.17% |
| Accessories | $749,307 | 17.30% | 21.33% | 12.05% |
| Copiers | $1,509,439 | 17.13% | 23.62% | 11.71% |
| Paper | $244,307 | 24.23% | 14.90% | 10.95% |

Tables is the only loss-making sub-category and combines a 29.07% average discount with a 57.61% loss rate. Chairs and Bookcases are profitable in aggregate but still carry loss rates above 29%.

### 8.3 Segment

Segment is not a useful margin differentiator:

| Segment | Sales share | Profit share | Margin | Loss share |
|---|---:|---:|---:|---:|
| Consumer | 51.48% | 51.00% | 11.51% | 24.59% |
| Corporate | 30.25% | 30.14% | 11.58% | 24.43% |
| Home Office | 18.27% | 18.86% | 11.99% | 24.10% |

The agent should not over-interpret the small Home Office advantage.

### 8.4 Market

| Market | Sales | Sales share | Profit share | Margin | Loss share | Avg. discount |
|---|---:|---:|---:|---:|---:|---:|
| APAC | $3,585,833 | 28.36% | 29.79% | 12.20% | 29.27% | 14.88% |
| EU | $2,938,139 | 23.24% | 25.38% | 12.69% | 21.18% | 10.31% |
| US | $2,297,354 | 18.17% | 19.50% | 12.47% | 18.72% | 15.62% |
| LATAM | $2,164,687 | 17.12% | 15.09% | 10.24% | 26.21% | 13.55% |
| EMEA | $806,184 | 6.38% | 2.99% | 5.45% | 31.93% | 19.61% |
| Africa | $783,776 | 6.20% | 6.05% | 11.34% | 22.45% | 15.67% |
| Canada | $66,932 | 0.53% | 1.21% | 26.62% | 0.00% | 0.00% |

EMEA is the weak major market: 6.38% of sales but only 2.99% of profit, with a 19.61% average discount. APAC is the largest market but has a 29.27% line-level loss rate, concentrated in Southeast Asia.

### 8.5 Region

Southeast Asia is a clear diagnostic slice:

| Region | Sales | Margin | Loss share | Avg. discount |
|---|---:|---:|---:|---:|
| Southeast Asia | $884,438 | 2.02% | 53.31% | 27.21% |
| EMEA | $806,184 | 5.45% | 31.93% | 19.61% |
| South | $1,600,960 | 8.77% | 25.40% | 14.91% |
| Oceania | $1,100,207 | 11.06% | 30.34% | 15.32% |
| Central | $2,822,399 | 11.03% | 25.02% | 13.89% |
| East | $678,834 | 13.48% | 19.42% | 14.54% |
| West | $725,514 | 14.94% | 9.93% | 10.93% |
| North | $1,248,192 | 15.59% | 19.04% | 9.61% |
| Central Asia | $752,839 | 17.60% | 13.33% | 6.75% |
| North Asia | $848,349 | 19.52% | 9.45% | 4.87% |

The contrast between North Asia (19.52% margin, 4.87% discount) and Southeast Asia (2.02% margin, 27.21% discount) is one of the strongest discount-policy signals in the data.

---

## 9. Geography

### 9.1 Top countries

| Country | Sales | Sales share | Margin | Loss share | Avg. discount |
|---|---:|---:|---:|---:|---:|
| United States | $2,297,354 | 18.17% | 12.47% | 18.72% | 15.62% |
| Australia | $925,257 | 7.32% | 11.40% | 28.62% | 14.35% |
| France | $858,930 | 6.79% | 12.69% | 15.78% | 7.23% |
| China | $700,591 | 5.54% | 21.51% | 2.87% | 1.43% |
| Germany | $628,857 | 4.97% | 17.07% | 12.20% | 5.70% |
| Mexico | $622,620 | 4.92% | 16.51% | 12.41% | 6.16% |
| India | $589,664 | 4.66% | 21.89% | 3.54% | 1.77% |
| United Kingdom | $528,570 | 4.18% | 21.17% | 12.98% | 6.57% |
| Indonesia | $404,887 | 3.20% | 3.86% | 58.99% | 29.73% |
| Brazil | $361,098 | 2.86% | 8.33% | 24.52% | 14.56% |

The top 10 countries contribute **62.63%** of sales. China, India, and the UK combine low discount rates with margins above 21%, while Indonesia combines a 29.73% average discount with a 3.86% margin.

### 9.2 Weak countries

| Country | Lines | Sales | Margin | Loss share | Avg. discount |
|---|---:|---:|---:|---:|---:|
| Kazakhstan | 101 | $4,602 | -154.28% | 100.00% | 70.00% |
| Nigeria | 905 | $54,347 | -148.58% | 100.00% | 70.00% |
| Turkey | 1,378 | $108,524 | -90.71% | 100.00% | 60.00% |
| Sweden | 203 | $30,490 | -57.46% | 99.01% | 50.79% |
| Netherlands | 435 | $77,520 | -52.98% | 95.40% | 48.18% |
| Ireland | 106 | $16,638 | -44.43% | 100.00% | 50.28% |
| Venezuela | 194 | $26,587 | -42.21% | 81.44% | 40.94% |
| South Korea | 172 | $33,133 | -38.61% | 87.79% | 45.29% |
| Pakistan | 249 | $58,873 | -38.13% | 87.55% | 44.46% |
| Haiti | 104 | $11,036 | -37.94% | 77.88% | 44.04% |

These are not marginal loss-makers. They are extreme-discount regimes where the average line loses money. The agent should route these into a diagnostic "loss driver" workflow rather than presenting them as normal market performance.

### 9.3 State and country pairing

There are 1,094 state values but **1,119 country-state pairs**; 16 state names occur in more than one country. Always group by `country + state`.

Examples:

| Country | State | Sales | Margin |
|---|---|---:|---:|
| United Kingdom | England | $485,166 | 20.59% |
| United States | California | $457,731 | 16.69% |
| France | Ile-de-France | $317,818 | 13.86% |
| United States | New York | $310,914 | 23.81% |
| Australia | New South Wales | $270,509 | 16.74% |
| United States | Texas | $170,187 | -15.12% |

Texas is a notable state-level loss driver despite being in the profitable US market.

---

## 10. Time patterns

| Year | Lines | Composite orders | Sales | Profit | Margin | AOV | Sales growth |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2011 | 8,998 | 4,516 | $2,259,511 | $248,940.81 | 11.02% | $500.33 | — |
| 2012 | 10,962 | 5,480 | $2,677,493 | $307,415.28 | 11.48% | $488.59 | 18.50% |
| 2013 | 13,799 | 6,890 | $3,405,860 | $408,512.76 | 11.99% | $494.32 | 27.20% |
| 2014 | 17,531 | 8,868 | $4,300,041 | $504,165.97 | 11.72% | $484.89 | 26.25% |

Growth is volume-led, not margin-led. Average order value is stable around $485-$500 while sales rise every year.

Seasonality:

- November and December contribute **24.77%** of total sales.
- Q4 contributes **34.01%** of sales.
- February is the weakest month at 4.30% of sales.
- Monday-Friday each contribute 17.16%-18.37% of sales.
- Saturday contributes 9.32%; Sunday contributes only 1.76%.

Sunday has a higher margin (13.28%) but too little volume to offset the weekday concentration.

---

## 11. Orders, products, and customer limits

### 11.1 Orders

- 25,754 composite order instances.
- Average order value: **$490.91**.
- Median order value: **$201.00**.
- 90th percentile order value: **$1,283.00**.
- Average lines per order: **1.99**.
- 8,742 orders, or **33.94%**, contain more than one category.

The gap between average and median order value shows a long tail of large orders. Use medians when describing a "typical" order.

### 11.2 Products

| Product | Sales | Profit | Margin |
|---|---:|---:|---:|
| Canon imageCLASS 2200 Advanced Copier | $61,600 | $25,199.93 | 40.91% |
| Nokia Smart Phone, with Caller ID | $30,042 | $5,455.95 | 18.16% |
| Fellowes PB500 Electric Punch Binding Machine | $27,454 | $7,753.04 | 28.24% |
| Cisco TelePresence System EX90 | $22,638 | -$1,811.08 | -8.00% |
| Nokia Smart Phone, Full Size | $22,261 | $8,121.48 | 36.48% |

High revenue does not imply high profit. Cisco TelePresence is the fourth-largest product by sales but loses money, and `FUR-CH-10002024` has $21,870 in sales with essentially zero profit. Product-level answers should rank by both revenue and profit, and should use `product_id`.

### 11.3 Customers are not measurable

Because `customer_name` is a reused label across countries, the following are unsupported without an external ID:

- Customer count
- New vs repeat customers
- Customer lifetime value
- Customer retention
- Customer-level discount response

Do not present `nunique(customer_name)` as a customer count.

---

## 12. What this means for the Data Analytics Agentic AI project

### 12.1 Semantic layer

The agent needs a small, explicit metric layer rather than free-form column selection.

| Metric | Canonical definition | Required guardrail |
|---|---|---|
| Line-item sales | `SUM(sales)` on the raw row grain | Never deduplicate by `order_id` |
| Order sales | `SUM(sales)` grouped by `order_key` | `order_id` alone collides across customers |
| Profit margin | `SUM(profit) / SUM(sales)` | Never average line-level margins |
| Loss rate | `COUNT(profit < 0) / COUNT(*)` | State that it is line-item based |
| Average discount | `AVG(discount)` at line grain | Do not call it an order-level discount |
| Shipping ratio | `SUM(shipping_cost) / SUM(sales)` | Use aggregate sums, not row ratios |
| Product count | `COUNT(DISTINCT product_id)` | `product_name` is not unique |
| Geography key | `country + state` | `state` alone is not globally unique |
| Customer count | Not supported | `customer_name` is not a customer ID |
| Net profit | Undefined | Confirm whether `profit` already includes `shipping_cost` |

### 12.2 Ingestion and validation tools

The agent should run deterministic checks before answering:

1. Confirm 51,290 rows and 21 source columns.
2. Confirm source-column missingness is zero.
3. Parse dates with `dayfirst=True` and assert no negative shipping gaps.
4. Reject month-first parsing and shipping gaps above 7 days.
5. Load `sales` with thousands separators and assert numeric dtype.
6. Recompute the composite order key and assert no cross-country collisions.
7. Assert `year == order_date.year`.
8. Assert 10,292 distinct `product_id` values and 147 countries.
9. Flag `profit < -sales`, zero-sales rows, and `shipping_cost > sales`.
10. Report whether the requested metric is line-level, order-level, or customer-level.

### 12.3 Query planning rules

- Default to the **line-item grain** for sales, profit, discount, product, category, and geography analysis.
- Use **`order_key`** for order counts, average order value, and multi-category order share.
- Use **`country + state`** for subnational geography.
- Use **`product_id`** for product performance.
- Use **`SUM(profit) / SUM(sales)`** for margin, not `AVG(profit_margin)`.
- Report **line-level loss rate** alongside aggregate profit.
- Separate **gross profit** from any **profit-after-shipping** calculation.
- Never count customers from `customer_name`.
- Never treat all money as comparable across countries without stating the currency assumption.

### 12.4 Response guardrails

The agent should include these caveats when relevant:

- "This is a synthetic, line-item dataset, not a customer-level database."
- "`order_id` is not unique; order metrics use the composite order key."
- "`customer_name` is reused across countries and cannot support customer counts."
- "Dates are parsed day-first because the file mixes slash and dash formats."
- "Profit may be pre-shipping; the file does not define net profit."
- "2,260 extreme loss lines are concentrated in 50%+ discounts and are treated as anomalies."
- "Correlation between discount and profit is descriptive, not causal."
- "There is no currency column; monetary comparisons assume a common currency."

### 12.5 High-value questions the agent can answer safely

- Which categories and sub-categories generate revenue but destroy profit?
- Which countries and regions are loss-making after controlling for discount?
- What is the profit breakpoint by discount band?
- How much does the 50%+ discount segment contribute to losses?
- Which products are high-revenue but negative-profit?
- How does order value distribution differ from the average?
- What is the seasonal shape of sales and profit?
- How do shipping speed, freight ratio, and margin relate?
- What share of orders are multi-category?
- How sensitive are results to excluding extreme loss lines?

### 12.6 Questions the agent should refuse or reframe

- "How many customers do we have?" — unsupported without a customer ID.
- "What is customer lifetime value?" — unsupported.
- "Which customer segment has the highest CLV?" — unsupported.
- "What is net profit after shipping?" — requires a confirmed profit definition.
- "What is the currency of these sales?" — no currency field.
- "Did discounts cause the losses?" — the data supports association, not causation.

---

## 13. Limitations

1. **Synthetic or semi-synthetic data.** The file contains repeated customer names, order ID collisions, and extreme profit values that are unlikely in a clean operational extract.
2. **No customer identifier.** Customer-level analysis is not possible.
3. **No currency field.** Cross-country monetary comparisons require an assumption.
4. **Profit definition is ambiguous.** `profit` and `shipping_cost` are separate fields, but the relationship is undocumented.
5. **No cost of goods, returns flag, order status, or campaign field.** Margin decomposition is limited to observed discount and product/country groupings.
6. **Order ID collisions.** `order_id` alone is not a valid grouping key.
7. **Product names are not unique.** Use `product_id`.
8. **State names are not globally unique.** Use `country + state`.
9. **Extreme loss lines.** 2,260 lines have profit below negative sales and should be isolated before averaging.
10. **No causal identification.** Discount, country, product, and margin are entangled; the analysis is descriptive.

---

## 14. Bottom line

- The dataset is a **51,290-line global sales table** with $12.64M in sales and an 11.62% aggregate margin.
- The correct analytical grain is the **line item**; the correct order key is the **composite order key**, not `order_id`.
- **Discount is the strongest observed profit lever.** Zero-discount lines are never loss-making; 50%+ discount lines are 99.71% loss-making.
- **Furniture and Tables are the clearest product-side problem.** Tables has a -8.47% margin and a 57.61% loss rate.
- **EMEA and Southeast Asia are the clearest geographic problems.** Nigeria, Turkey, and the Netherlands are extreme loss regimes driven by very high discounts.
- **The customer dimension is not usable** without an external customer ID.
- **Profit vs shipping must be defined before any net-profit metric is published.**
- The agent should treat this dataset as a **descriptive, line-item, synthetic extract**, not as a causal or customer-level source of truth.
