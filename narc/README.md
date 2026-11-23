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
  绝对值之后的后缀可读。

## 目录

```
narc/
  format.py     线格式常量与 struct 格式
  io_utils.py   带读取台账和范围强制的 pread 封装
  catalog.py    全量编目、结构校验、来源身份绑定
  query.py      窄窗查询与重建状态机
  writer.py     仅用于构造样例/测试的归档写入器
  cli.py        catalog / query / verify 命令
narc_tool.py    支持 python3 -I 的顶层入口
tests/
  reference.py  独立实现的全量参考解码器
  test_narc.py  差分、错误类别、长链、边界与字节审计测试
```

## 命令行

```bash
python3 -I narc_tool.py catalog  <archive.narc> <catalog.json>
python3 -I narc_tool.py query    <catalog.json> --start 100 --end 110 \
                                 --columns 1,2 [--full-verify] [--format json]
python3 -I narc_tool.py verify   <catalog.json> [archive.narc]
```

查询输出末尾给出 `bytes_read`、物理读区间 `spans` 和触达的块标识，可直接用于
读取边界审计。

## 库接口

```python
from narc import build_catalog, query_window

build_catalog("feed.narc", "feed.json")
result = query_window("feed.json", start_row=1000, end_row=1008, columns=[1, 4])
for cell in result.cells:
    print(cell.row, cell.column_id, cell.status, cell.value, cell.block_id)
print(result.bytes_read, [(s.offset, s.end) for s in result.spans])
```

## 测试

```bash
python3 -I tests/test_narc.py
```

测试包含：

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

## 设计取舍

- 编目只保存标量摘要、稀疏检查点和前缀缺测索引，不保存解码行流；
- 检查点步长固定 256 条记录，窄窗读取量有明确上界而不是仅靠耗时对比；
- 所有重建使用宿主语言任意精度整数，64 位限制只存在于线格式记录负载；
- 损坏块的判断在编目时完成，查询路径不再对其负载做任何 I/O。
