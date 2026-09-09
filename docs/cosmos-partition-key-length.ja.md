# ScalarDB における Cosmos DB パーティションキー長制約の調査

## サマリ

- Azure Cosmos DB のパーティションキー値には長さ制約があり、既定(V1 ハッシュ)では **101 バイト**、コンテナ作成時に Large partition keys(V2 ハッシュ)を有効化した場合は **2048 バイト**まで。
- ScalarDB の Cosmos アダプタは、すべてのパーティションキーカラムを 1 本の文字列に連結し、それを Cosmos のパーティションキー値として使う。
- Java v4 Cosmos SDK(`com.azure:azure-cosmos`、現在 `4.81.0` に固定)は、呼び出し側が明示的に `PartitionKeyDefinitionVersion.V2` を設定しない限り Large partition keys を有効化しない。ScalarDB はこの設定を行っていないので、**ScalarDB が作成するコンテナはすべて V1(101 バイト)ハッシュ**である。
- ScalarDB は連結後のパーティションキー長を **一切検証していない**。結果として V1 の 101 バイト超過は書き込み成功のままサイレントにハッシュ衝突を起こし、V2 の 2048 バイト超過は(ユーザが手動で V2 化していた場合)Cosmos の生 `BadRequest` として浮上する(ScalarDB 側で分かりやすいエラーには変換されない)。

---

## 背景: Cosmos DB のパーティションキー長

Azure Cosmos DB はハッシュベースのパーティション方式を採用しており、ハッシュ関数には 2 つのバージョンがある:

| バージョン | ハッシュに使うバイト数 | 上限を超えた場合の挙動 |
|---|---|---|
| **V1**(2019-05-03 以前に作成されたコンテナ、および V2 にオプトインしない SDK で作成されたコンテナの既定) | 先頭 101 バイト | 書き込み自体は成功するが、先頭 101 バイトが同じ複数のキーは**同一の論理パーティション**として扱われる。結果として: ハッシュ衝突、論理パーティションサイズクォータの誤計上、一意インデックスの誤適用、ストレージ分散の偏りが発生する。 |
| **V2**(Large partition keys) | 全体(最大 2048 バイト) | 2048 バイトを超える値はサービス側で `BadRequest` として**拒否される**。値全体をハッシュ計算に使うのでサイレントな衝突は起きない。 |

参考:

- [大きなパーティションキーを使用してコンテナーを作成する](https://learn.microsoft.com/ja-jp/azure/cosmos-db/large-partition-keys)
- [サービスクォータと既定の制限](https://learn.microsoft.com/ja-jp/azure/cosmos-db/concepts-limits) — Per-item limits 表に "Maximum length of partition key value: 2,048 bytes (101 bytes if large partition-key isn't enabled)" と明記されている。

Large partition keys はコンテナ作成時にしか設定できず、既存コンテナに後から適用することはできない。

## どの SDK が V2 をデフォルトにしているか

Microsoft のドキュメント記載:

- **Azure Portal**: Portal 経由で作成したコンテナは V2 がデフォルト。
- **.NET SDK V3**: V2 がデフォルト。
- **.NET SDK V2**: V1 がデフォルトで、V2 は明示指定が必要。
- **Java v4 SDK**(`com.azure:azure-cosmos`): ドキュメントには明記されていない。"Supported SDK versions" 表に載っているのは Java Sync 2.4.0 / Java Async 2.5.0 という旧世代 SDK の最小サポートバージョンのみで、v4 SDK が自動で V2 になるとは書かれていない。

Java v4 SDK のソース(`azure-cosmos-4.81.0`)を確認すると:

1. `PartitionKeyDefinition()` コンストラクタは `kind=HASH` だけを設定し、`versionOptional` は `null` のまま。
2. `CosmosContainerProperties(id, partitionKeyPath)` は素の `PartitionKeyDefinition` を作って `paths` と `kind` だけを設定 — バージョンには触らない。
3. `PartitionKeyDefinition.populatePropertyBag()` は `versionOptional` が null または empty の場合、**JSON に `version` フィールドを含めない**ため、バージョンは通信路上に送られない。
4. `ModelBridgeInternal.isV2()` は version が未設定なら false を返す(SDK 内部でも V2 扱いされない)。

バージョン未指定時のサーバ側デフォルトは後方互換のため V1。したがって Java v4 SDK は、呼び出し側が明示的に `PartitionKeyDefinitionVersion.V2` を指定しない限り **V1 のコンテナを作成する**。

Microsoft ドキュメントの「2019 年 5 月 3 日より前に作成されたすべての Azure Cosmos DB コンテナーでは、パーティション キーの最初の 101 バイトに基づいてハッシュを計算するハッシュ関数が使われています」という文言は、**V2 が opt-in の選択肢として導入されたタイミング**を示しているだけで、その日以降にデフォルトが V2 に切り替わったわけではない。

## ScalarDB の現状

### コンテナ作成([`CosmosAdmin`](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosAdmin.java))

`CosmosAdmin.computeContainerProperties()` は以下のように書かれている:

```java
return new CosmosContainerProperties(table, PARTITION_KEY_PATH)
    .setIndexingPolicy(indexingPolicy);
```

`PartitionKeyDefinitionVersion` の指定がないため、作成される Cosmos コンテナは V1 ハッシュ(101 バイト制限)になる。

### パーティションキー値の組み立て([`CosmosOperation`](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperation.java), [`ConcatenationVisitor`](../../core/src/main/java/com/scalar/db/storage/cosmos/ConcatenationVisitor.java))

Cosmos オペレーション実行時、`CosmosOperation.getConcatenatedPartitionKey()` はメタデータ順にパーティションキーカラムを走査し、`:` を区切り文字として文字列化した値を連結する:

- 数値・boolean 系: `String.valueOf(...)`
- TEXT: 生の UTF-8(`:` は `CosmosOperationChecker` で事前に禁止)
- BLOB: URL-safe Base64(パディングなし)。バイト長は約 1.33 倍に膨らむ。
- Date / Time / Timestamp / TimestampTZ: エポック相当の数値エンコード

この連結結果が Cosmos のパーティションキー値として送信される。

### 検証([`CosmosOperationChecker`](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperationChecker.java))

`CosmosOperationChecker` の primary key 系チェックは以下のみ:

- TEXT カラムの禁止文字チェック(`:` `/` `\` `#` `?`)
- BIGINT の範囲チェック(±2^53)

各パーティションキーカラム値の長さも、連結後の長さも **検証していない**。

### Schema Loader([`CosmosCommand`](../../schema-loader/src/main/java/com/scalar/db/schemaloader/command/CosmosCommand.java))

Cosmos サブコマンドが受け付けるオプションは以下のみ:

- `-h/--host` (URI)
- `-p/--password` (Key)
- `-r/--ru` (Request Units)
- `--no-scaling` (autoscale 無効化)
- `-D/--delete-all`, `--repair-all`, `-A/--alter` (モードフラグ)

Large partition keys を有効化するオプションは **存在しない**。

## 問題点

### 1. サイレントなパーティションキーハッシュ衝突(V1 の制限)

ScalarDB が作成するコンテナはすべて V1(101 バイト)ハッシュのため、連結後のパーティションキーの先頭 101 バイトが同じ 2 つのレコードは同じ論理パーティションに落ちる。これにより:

- 論理的に別のはずのパーティションキーが 1 つのパーティションとしてサイレントに扱われる。
- 論理パーティションのストレージクォータ(20 GB)計算が、意図とは違う"衝突により膨らんだ"パーティション単位で計上される。
- 一意インデックスが、ユーザが別物とみなす値をまたいで適用される。
- ストレージ分散が偏る。

どの層からもエラーは上がらず、書き込みは成功する。問題は容量や整合性の異常として後から現れる。

ScalarDB で衝突が起きうる現実的なシナリオ:

- 長い TEXT カラムをパーティションキーにするケース(URL、JSON 片、共通プレフィックスを持つ自然キー、など)。
- BLOB カラムをパーティションキーにするケース: Base64 は 3 バイトを 4 文字に膨らませるので、生の BLOB が 76 バイトを超えると単体で 101 バイト圏に入ってしまう。
- 複合パーティションキーで、1 コンポーネントが長さのバジェットを支配してしまうケース(例: `tenant-long-name:short-user-id`)。

### 2. V2 用の長さ事前チェックもない

ユーザが手動でコンテナを V2 として再作成した場合でも、ScalarDB は 2048 バイトの制限を検証しない。2048 バイトを超えるパーティションキーの書き込みは Cosmos の生 `BadRequest` として浮上する。エラーメッセージは内部の `concatenatedPartitionKey` プロパティに言及し、ScalarDB のユーザから見たカラム構造とは対応しないため、原因の特定が難しい。

### 3. Schema Loader に Large Partition Key 用のオプションがない

リスクを理解しているユーザであっても、Large partition keys を有効化した ScalarDB テーブルを first-class な手段で作成することはできない。Portal 等で手動でコンテナを作るのは Schema Loader をバイパスする裏道であり、標準の運用パスではない。

### 4. ドキュメントに制約の記載がない

101 バイト制限とサイレント衝突の挙動は、ScalarDB の Cosmos 関連ドキュメントに記載されていない。ユーザは実際に踏むまでこの制約を予期できない。

## 推奨対応

インパクトと複雑さの降順で:

### A. 新規作成するコンテナで V2 をデフォルトにする

`CosmosAdmin.computeContainerProperties()` を修正し、`PartitionKeyDefinitionVersion.V2` を明示指定する:

```java
PartitionKeyDefinition partitionKeyDefinition =
    new PartitionKeyDefinition()
        .setKind(PartitionKind.HASH)
        .setPaths(Collections.singletonList(PARTITION_KEY_PATH))
        .setVersion(PartitionKeyDefinitionVersion.V2);
return new CosmosContainerProperties(table, partitionKeyDefinition)
    .setIndexingPolicy(indexingPolicy);
```

トレードオフ:

- **既存コンテナは影響を受けない**。V1 か V2 かはコンテナ作成時にしか設定できないため、既存デプロイのユーザが恩恵を受けるには container copy によるマイグレーションが必要。
- Microsoft は新規コンテナには V2 を推奨しており、Portal も V2 がデフォルトになっている。ScalarDB を V2 デフォルトに揃えるのはエコシステムのベストプラクティスと整合する。

### B. `CosmosOperationChecker` に事前の長さチェックを追加する

連結後のパーティションキー長がコンテナのバージョン別上限(V1 なら 101、V2 なら 2048 バイト)を超えている場合、`COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG` のような ScalarDB 独自エラーで拒否する。

これにより、V1 のサイレント衝突と V2 の不透明な `BadRequest` を、明示的で対処可能なエラーに変換できる。理想的には、メタデータロード時に `CosmosContainerProperties.getPartitionKeyDefinition().getVersion()` を読んで、コンテナがどちらのバージョンで作成されたかを把握したうえでチェックする形が良い。

### C. Schema Loader に Large Partition Key オプションを追加する

デフォルトを V2 に変える(A)がインパクトが大きすぎると判断される場合、`CosmosCommand` にオプトインオプション(例: `--large-partition-key`)を追加し、`CosmosAdmin.createTable(... , options)` にパススルーする。

### D. 制約をドキュメントに記載する

A/B/C のいずれを採るにせよ、Cosmos ストレージ関連ドキュメントに以下の説明を追加すべき:

- V1 の 101 バイト / V2 の 2048 バイト制限。
- ScalarDB がパーティションキーカラムを `:` で連結し、BLOB は Base64 でエンコードするため、複合キーや BLOB キーでは実質的な生値バジェットが 101 バイトより小さくなる旨。
- 長いパーティションキーを扱う必要があるユーザ向けのガイダンス。

## 付録: コードリファレンス

| 対象 | ファイル | キーとなるシンボル |
|---|---|---|
| コンテナ作成 | [core/.../CosmosAdmin.java](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosAdmin.java) | `computeContainerProperties()`, `PARTITION_KEY_PATH` |
| パーティションキー連結 | [core/.../CosmosOperation.java](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperation.java) | `getConcatenatedPartitionKey()` |
| カラムの文字列化 | [core/.../ConcatenationVisitor.java](../../core/src/main/java/com/scalar/db/storage/cosmos/ConcatenationVisitor.java) | `build()`, 型別 `visit(...)` |
| Primary key バリデーション | [core/.../CosmosOperationChecker.java](../../core/src/main/java/com/scalar/db/storage/cosmos/CosmosOperationChecker.java) | `PRIMARY_KEY_COLUMN_CHECKER` |
| Cosmos エラーコード | [core/.../CoreError.java](../../core/src/main/java/com/scalar/db/common/CoreError.java) | `COSMOS_*` |
| Schema Loader Cosmos コマンド | [schema-loader/.../CosmosCommand.java](../../schema-loader/src/main/java/com/scalar/db/schemaloader/command/CosmosCommand.java) | `call()` |
| Cosmos SDK バージョン | [build.gradle](../../build.gradle) | `azureCosmosVersion = '4.81.0'` |
