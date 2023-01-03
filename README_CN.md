# openGemini-castor：时序数据分析Python库
![License](https://img.shields.io/badge/license-Apache2.0-green) 

[简体中文]() | [English](README.md)

## 关于 openGemini-castor
openGemini-castor 是一个时序数据分析Python库。它提供了一个端到端的时序分析框架，可用于智能运维、物联网等场景，具备高效的实时数据分析和灵活的算法编排能力。

主要特性：
* 高性能分析：每秒万级实时指标数据并发检测。
* 检测器多样性：针对数据突升突降，持续上升/下降，超过阈值等不同场景，提供了多个异常检测器。
* 流式检测：多种异常检测器都对流式异常检测做了相应的支持。完成流式数据的实时检测，只需缓存少量数据且每次不必输入大量历史数据，从而可以有效地缓解计算压力。
* 告警抑制：针对不同场景，可以组合使用瞬时告警抑制器，持续告警抑制器，门限告警抑制器和变化率告警抑制器，以抑制误报和重复告警。
* 严重程度分级：根据多种异常出现情况，按严重程度对异常进行分类。辅助开发者对故障严重程度的判断。
* 灵活的算法编排：可通过配置参数灵活地制定检测流程。其中，异常检测器、告警抑制器和严重程度分级器都可以根据不同的需求进行配置。

了解更多详细信息，可参考[openGemini-castor使用指南](https://github.com/openGemini/community/blob/main/openGemini-castor_user_manual.md)
## 安装
Python版本要求：3.9.1

通过源代码完成安装：
```shell
> git clone https://github.com/openGemini/openGemini-castor.git
> cd openGemini-castor
> python3 setup.py install
```

## 快速开始
为了帮助开发者快速使用openGemini-castor，本章节提供了使用 "DFFIENTIATEAD" 和 "ThresholdAD" 算法检测异常的例子。

#### 准备检测数据和参数
首先我们需要准备检测数据以及配置参数。其中，参数可以通过加载默认配置文件来获取。
```python
from castor.utils.base_functions import load_params_from_yaml
import pandas as pd
data = pd.read_csv("./tests/data/fluctuate.csv", index_col="time", parse_dates=True)
params = load_params_from_yaml(config_file="./conf/detect_base.yaml")
```

#### 检测时序数据异常

指定 "DIFFERENTIATEAD" 和 "ThresholdAD" 检测器后，初始化模型并进行检测。

```python
from castor.detector.pipeline_detector import PipelineDetector
from adtk.visualization import plot
from castor.utils import const as con
import matplotlib.pyplot as plt
algo_list = ["DIFFERENTIATEAD", "ThresholdAD"]
pipeline_detector = PipelineDetector(algo_list, params)
results = pipeline_detector.run(data)
for algo, anomalies in zip(algo_list, results):
    labels = anomalies.get(con.LABEL)
    ax = plot(data, anomaly=labels, anomaly_color="red")
    ax[0].set_title(algo)
plt.show()
```
如果在配置参数中将告警抑制器取消（如detect_base_without_suppressor.yaml文件中的配置），那么结果会包含异常检测器所检测到的全部异常：
<img src="./docs/_figure/DIFFERENTIATEAD_without_suppressor.jpg" />
<img src="./docs/_figure/ThresholdAD_without_suppressor.jpg" />

如果完整使用默认配置文件，openGemini-castor 会根据配置参数所指定告警抑制器进行告警抑制，即部分异常会从结果中剔除，减少误报和重复报警：
<img src="./docs/_figure/DIFFERENTIATEAD.jpg" />
<img src="./docs/_figure/ThresholdAD.jpg" />


## 贡献
[Tips for Contribution](https://github.com/openGemini/openGemini/blob/main/CONTRIBUTION_CN.md)

## License
openGemini-castor 采用 Apache 2.0 license. 详细见 [LICENSE](https://github.com/openGemini/openGemini-castor/blob/main/LICENSE).




