function loadGrid(){
  $.ajax({
    url: "http://0.0.0.0:8080/status", 
    dataType: "json", 
    type: "GET"
  }).done(function(json) {
    $(".tooltip").remove()
    $("#grid").children("div").remove()
    active = false
    $.each(json.slaves, function(id, data){ 
      // Prepare grid elements
      if (data.info == undefined) return true
      tooltip_info = '<b>'+data.info.hostname+'</b><br /><br />'+'Memory: '+data.info.mem+'<br />'
        +'Connected: '+data.info.connected
      node_class = (data.tasks > 0) ? 'grid_elem_active' : 'grid_elem_inactive'
      node_link = '<a class="'+node_class+'" id="'+id+'"'+' href="'+data.info.url+'"'+
        ' target=_new'+' title="'+tooltip_info+'">'+data.tasks+"/"+data.info.cpus+'</a>'
      mem_bar = '<div class="grid_elem_mem_bar"><div></div></div>'

      // Add grid element + memory bar
      grid_elem = $('<div class="grid_elem"></div>')
        .appendTo('#grid')
        .append(node_link)
        .append(mem_bar)
      if (data.mem != null)
        grid_elem.children('.grid_elem_mem_bar')
          .children('div')
          .css('height', data.mem.perc+'%')
      $('#'+id).tooltip()
    })

    // Update cluster status + memory usage
    nodes_cpus = json.slaves.total_nodes+' nodes, '+json.slaves.total_cpus+' CPUs'
    if (active) {
      $("#status_bar")
        .removeClass('prog_bar_inactive')
        .addClass('prog_bar_active')
      $('#status_msg').html(nodes_cpus+' [ Running: '+json.slaves.total_tasks+' active jobs ]')
    } else {
      $("#status_bar")
        .removeClass('prog_bar_active')
        .addClass('prog_bar_inactive')
      $('#status_msg').html(nodes_cpus+' [ Idle ]')
    }
    $('#mem_status').html('Cluster memory: '+json.slaves.total_mem_used+'GB of '
      +json.slaves.total_mem_cap+'GB used')
  })
}
