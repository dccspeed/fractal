set terminal pdfcairo font "Times-New-Roman,12"
set output 'speedup.pdf'
set key inside right top vertical Right noreverse noenhanced autotitles nobox
set datafile missing '-'
set style data linespoints
#set xtics border in scale 1,0.5 nomirror rotate by -45  offset character 0, 0, 0 autojustify
#set xtics norangelimit font ",8"
#set xtics autofreq 5
#set xtics 1,60
#set yrange [1:100]
set xlabel "Samples"
set ylabel "Th (bytes/second)"
f(x) = x
plot f(x), \
   'speedup.gp.dat' with yerrorbars, '' with lines
