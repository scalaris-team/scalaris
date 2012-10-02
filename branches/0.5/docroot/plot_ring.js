function plot_ring(ring, colors) {
 $.plot($("#ring"), ring, {
  series: {
   pie: {
    show: true,
    radius: 0.9,
    innerRadius: 0.4,
    label: {
     show: true,
     radius: 1.0,
     formatter: function(label, series) {
      return '<div style="font-size:8pt;text-align:center;padding:2px;color:white;">'+label+'</div>';
     },
     background: { opacity: 0.5, color: '#000000' },
     threshold: 0.03
    }
   }
  },
  legend: { show: false },
  grid: { hoverable: true, clickable: true },
  colors: colors
 });
 $("#ring").bind("plothover", pieHover);
 $("#ring").bind("plotclick", pieClick);
}

function pieHover(event, pos, obj) {
 if (!obj)
  return;
 percent = parseFloat(obj.series.percent).toFixed(2);
 $("#ringhover").html('<span style="font-weight: bold">'+obj.series.label+' ('+percent+'%)</span>');
}

function pieClick(event, pos, obj) {
 if (!obj)
  return;
 percent = parseFloat(obj.series.percent).toFixed(2);
 alert(''+obj.series.label+': '+percent+'%');
}
