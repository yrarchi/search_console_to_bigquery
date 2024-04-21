# search_console_to_bigquery

以下のドキュメントで示されている Search Console データのエクスポート機能（以下「公式エクスポート機能」）では、設定以後の期間のデータが毎日エクスポートされるが、過去期間のデータはエクスポートされない。  
[BigQuery への Search Console データの一括エクスポートについて](https://support.google.com/webmasters/answer/12918484)

このリポジトリでは、過去期間のデータについても公式エクスポート機能でのエクスポート結果に近づけた形で取得・整形し、BigQueryに挿入を行う。

以下の2テーブルを対象とする。
- searchdata_site_impression
- searchdata_url_impression

### Getting Started
前提：Google Cloud でプロジェクトを作成し、BigQuery API と Google Search Cosole API を有効にしている

#### 1. サービスアカウントの作成と権限付与
- サービスアカウントを作成し、以下の権限を付与する
```
bigquery.datasets.get
bigquery.jobs.create
bigquery.tables.get
bigquery.tables.getData
bigquery.tables.updateData
```
- Search Console の `設定 > ユーザーと権限` より、上記で作成したサービスアカウントを登録する

#### 2. テーブルの作成
searchdata_site_impression と searchdata_url_impression に相当する過去のデータを入れるテーブルを作成する。テーブルのスキーマは schema/schema_site.json と schema/schema_url.json の定義で作成する。

#### 3. パラメータの設定
以下のパラメータを config.json で指定する
- target_start_date": 取得対象の開始日
- "target_end_date": 取得対象の終了日
- "days": 1日に取得する日数
- "site_url": Serach Console の対象サイトURL
- "dataset_id": データを挿入するテーブルのあるデータセット名
- "site_table": searchdata_site_impression に相当するデータを挿入するテーブル名
- "url_table": searchdata_url_impression に相当するデータを挿入するテーブル名

※ Search Console API の1日あたりの取得レコード数に制限があるため、daysはそれを超えない範囲に調整する

#### 4. Cloud Functions の設定
以下のファイルを Cloud Functions に設定する
- search_console_to_bigquery.py
- config.json
- requirements.txt

Cloud Functions の実行は 1.で作成したサービスアカウントで行う。Search Console API の1日あたりの取得レコード数に制限があるため、トリガーを Cloud Pub/Sub にし、Cloud Scheduler で1日1回実行するようにする。

### 注意点
- 1日あたりの実行で最大25,000レコードのAPIの制限があり、それを超える Search Console のデータを有するケースには対応していない
-  Search Console データのエクスポート機能による集計になるべく近づけているが、以下の点については対応していない
  - 匿名化したクエリを対象とした集計
  - 検索での見え方のタイプを対象とした集計
  - Discover, Googleニュースアプリの検索結果に関する集計
