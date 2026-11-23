# narc — 仪器归档离线编目与窄窗读取

为定长二进制仪器归档提供离线编目和按样点区间/列的精确窄窗查询。不使用任何
通用压缩库；线格式见 [FORMAT.md](FORMAT.md)。

## 能力边界

- 查询指定半开样点区间和列，逐位置返回精确值或
  `missing` / `damaged` / `missing_prefix` / `uncovered`，并标注所用块标识；
- 编目绑定不可变来源身份（大小 + 整文件 SHA-256），换版本必须重新编目；
- 窄窗读取只用 `pread` 访问目标块负载，台账记录每个物理读区间和实际字节数；
- 重复小查询从稀疏检查点或入口可信值锚定，不从文件头/列首顺扫，不读无关列；
- 截断、摘要不符、非法行的块整体不可读；下游不能跨过坏前缀借值，但块内可信
  绝对值之后的后缀可读；
- 离线发布（`release`）把获准样点重打包成一批可独立解码的 v1 分件，原档、原
  来源名与解码垫用的私有前值都不外发。

## 目录

```
narc/
  format.py     线格式常量与 struct 格式
  io_utils.py   带读取台账和范围强制的 pread 封装
  catalog.py    全量编目、结构校验、来源身份绑定
  query.py      窄窗查询与重建状态机
  release.py    离线发布：授权单校验、合规分件规划与原子交付
  writer.py     仅用于构造样例/测试的归档写入器
  cli.py        catalog / query / verify / release 命令
narc_tool.py    支持 python3 -I 的顶层入口
examples/
  release/      可复跑的离线发布小样例（构造源档→发布→逐件独立解码）
tests/
  reference.py  独立实现的全量参考解码器
  test_narc.py  差分、错误类别、长链、边界与字节审计测试
  test_release.py 发布语义、拒绝规则、规划最优性与 O(N log N) 测试
```

## 命令行

```bash
python3 -I narc_tool.py catalog  <archive.narc> <catalog.json>
python3 -I narc_tool.py query    <catalog.json> --start 100 --end 110 \
                                 --columns 1,2 [--full-verify] [--format json]
python3 -I narc_tool.py verify   <catalog.json> [archive.narc]
python3 -I narc_tool.py release  <archive.narc> <authorization.json> <delivery/>
```

查询输出末尾给出 `bytes_read`、物理读区间 `spans` 和触达的块标识，可直接用于
读取边界审计。

## 库接口

```python
from narc import build_catalog, query_window, release_archive

build_catalog("feed.narc", "feed.json")
result = query_window("feed.json", start_row=1000, end_row=1008, columns=[1, 4])
for cell in result.cells:
    print(cell.row, cell.column_id, cell.status, cell.value, cell.block_id)
print(result.bytes_read, [(s.offset, s.end) for s in result.spans])

# 离线发布：返回 parts.json 的内容，并把分件原子写入新目录。
manifest = release_archive("feed.narc", "authorization.json", "delivery/")
```

## 离线发布（release）

`release` 面向“把获准共享的样点交给外部团队”的场景：输入一个不再变化的
v1 原档、一份 JSON 授权单和一个**尚不存在**的交付目录，产出一批接收端用现有
`catalog` / `query` 就能单独打开的 v1 文件，外加 `parts.json`。

### 授权单

```json
{
  "fragment_id": 7,
  "columns": [1, 2],
  "ranges": [[0, 12], [14, 16]],
  "max_bytes": 200
}
```

- `fragment_id`：唯一指定片段，分件只保留这一个片段身份；
- `columns`：v1 列编号，非空；重复列按一列对待；
- `ranges`：半开样点区间 `[start,end)`，非空且必须落在指定片段内；重叠或相邻
  的区间合并为同一个授权区间；
- `max_bytes`：每个分件（含文件头、片段头、块头、负载和结束标记）的字节上限，
  取值范围 `42` 至 `4294967295`。

### 交付物与安全语义

- `parts.json` 按样点顺序列出每件的 `file`、半开区间、实际 `bytes` 和
  `sha256`；各件区间互不重叠，合起来恰好覆盖授权区间；
- 每件都是自描述 v1 文件，片段来源名统一为 `release`，只含授权片段和请求列；
- 获准列的精确值、显式缺测（`missing`）和无覆盖（`uncovered`）原样保留——
  无覆盖位置没有任何块拥有，绝不写成缺测；
- 每个块都是**独立块**，块内第一条值记录是取自获准样点的绝对值锚，因此分件
  不依赖任何其它分件，也不夹带为解码垫用的私有前值。重建所需的原档前置上下文
  只在发布端内存中使用；
- 不夹带未授权行/列记录、原档路径或原来源名；
- 重建值不受 int64 限制：超过有符号 64 位的精确值仍以增量链保留，绝不截断，
  也不虚构一条假样点作锚（无法在授权范围内锚定的整单拒绝）；
- 获准位置出现坏块、或缺可信前置（`missing_prefix`）时**拒绝整单**，不把它们
  写成缺测；未选到的位置有坏块不单独构成拒绝理由；
- 无解、输入无效或写入失败时不留下最终交付目录：所有写入先落在目标旁的隐藏
  临时目录，全部成功后才一次 `rename` 落位，接收端不可能先拿到部分分件。

### 规划目标与复杂度

只要存在合规切法就会发布。规划在合规方案中先最小化**件数**，件数相同时按样点
顺序取**终点序列字典序最大**的方案。样点号之间可能空出数亿行，规划完全基于
事件（授权区间、请求列、获准覆盖记录），不随纯空档行数增长；分件规划为近线性
`O(N log(N+1))`（逐段二进制倍增跳转表 + 全局边界上的单调双指针扫描）。

### 可复跑样例

```bash
bash examples/release/run_example.sh
```

脚本会构造源档与授权单、执行发布，再对每个分件单独 `catalog` / `query`，验证
它们脱离彼此即可解码，并确认原来源名不出现在任何交付字节中。

## 测试

```bash
python3 -I tests/test_narc.py
python3 -I tests/test_release.py
```

`test_narc.py` 包含：

1. **小数据全量参考核对**：独立参考解码器对所有 30 行 × 3 列、全部
   `465` 个非空窗口做穷举差分，逐位置比较值与四类错误状态；
2. **长增量链末端**：4096 条记录的链只在尾部查询，断言单次窄读、读取上界和
   精确末值；
3. **块中途绝对值**：窗口落在绝对值前后，验证后缀无需读块首即可重建、前缀
   增量为 `missing_prefix`；
4. **孤立坏列**：摘要损坏、截断、非法标签分别注入，验证整块损坏、下游隔离与
   健康列不受影响；
5. **投影与读取边界**：断言所有物理读都落在目标列负载内，重复查询字节数稳定；
6. **身份绑定**：同尺寸篡改被完整哈希捕获，尺寸变化被每次查询的廉价检查拒绝。

`test_release.py` 包含：获准位置的值/缺测/无覆盖逐项对齐、分件脱离彼此独立
解码（含块中途切分与依赖块私有前值恢复）、坏块与缺可信前置整单拒绝、int64
超限精确值保留、对 `60` 组随机用例用暴力 DP 核对“最少件数 + 字典序最大终点”
规划、授权单校验、原子交付与泄漏检查，以及数亿行空档下规划不随行数增长。

## 设计取舍

- 编目只保存标量摘要、稀疏检查点和前缀缺测索引，不保存解码行流；
- 检查点步长固定 256 条记录，窄窗读取量有明确上界而不是仅靠耗时对比；
- 所有重建使用宿主语言任意精度整数，64 位限制只存在于线格式记录负载；
- 损坏块的判断在编目时完成，查询路径不再对其负载做任何 I/O；
- 发布只复用编目元数据做定位，解码读取被限制在与授权相交的可读块负载内，
  分件全部重编码为独立块，原档结构、来源身份与路径均不进入交付目录。
