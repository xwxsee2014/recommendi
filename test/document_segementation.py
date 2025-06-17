from modelscope.outputs import OutputKeys
from modelscope.pipelines import pipeline
from modelscope.utils.constant import Tasks

p = pipeline(
    task=Tasks.document_segmentation,
    model='iic/nlp_bert_document-segmentation_chinese-base', model_revision='master')

doc = """
# Ꮵ
对于上面的问题，你还能列出其他方程吗？ 如果能，你依据的是哪个相等关系？
列方程时，要先设字母表示未知数，然后根据问题中的相等关系，写出含有未知数的等式——方程（equation）
例1　根据下列问题，设未知数并列出方程：
（1）用一根长 $2 4 \ \mathrm { c m }$ 的铁丝围成一个正方形，正方形的边长是多少？（2）一台计算机已使用 $1 ~ 7 0 0 ~ \mathrm { h }$ ，预计每月再使用 $1 5 0 \mathrm { ~ h ~ }$ ，经过多少个月这台计算机的使用时间达到规定的检修时间 $2 ~ 4 5 0 ~ \mathrm { h ? }$ （3）某校女生占全体学生数的 $52 \%$ ，比男生多80人，这个学校有多少学生？
解：（1）设正方形的边长为 $x$ cm
列方程
$$
4 x = 2 4 .
$$
（2）设 $x$ 个月后这台计算机的使用时间达到$2 ~ 4 5 0 \mathrm { h }$ ，那么在 $x$ 个月里这台计算机使用了 $1 5 0 x { \mathrm { ~ h ~ } }$
列方程
![](images/27e0f15bf695fbb569b07380282809bac656fa7a4c00d6c83fadb2399823855f.jpg)
$$
1 ~ 7 0 0 + 1 5 0 x { = } 2 ~ 4 5 0 .
$$
（3）设这个学校的学生数为 $x$ ，那么女生数为$0 . 5 2 x$ ，男生数为 $( 1 - 0 . 5 2 ) x$ .
列方程
$$
0 . 5 2 x - ( 1 - 0 . 5 2 ) x = 8 0 .
$$
上面各方程都只含有一个未知数 （元），未知数的次数都是1，等号两边都是整式，这样的方程叫做一元一次方程 （linearequationwithoneunknown）
"""

result = p(documents=doc)

print(result)
