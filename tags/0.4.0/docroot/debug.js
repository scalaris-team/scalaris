function updateTable(group, id, tablePanel){
    tablePanel.reconfigure(createStore(group, id), 
			   new Ext.grid.ColumnModel([
		{header: 'Key', width: 120, sortable: false, dataIndex: 'key'},
		    {header: 'Value', width: 555, sortable: false, dataIndex: 'value'}
		    ]));
    tablePanel.render();
}

function createStore(group, id){
    return new Ext.data.JsonStore({
	    autoLoad: true,
		url: 'processinfo.yaws?group=' + group + '&id=' + id,
		root: 'pairs',
		fields: ['key', 'value']
		});
}

Ext.onReady(function() {
	// Note: For the purposes of following along with the tutorial, all 
	// new code should be placed inside this method.

	      var processTreeLoader = new Ext.tree.TreeLoader({
		baseParams: {requestAction: 'processTree'},
		    dataUrl: 'processtree.yaws'
		});

	      var rootNode = new Ext.tree.TreeNode();
	      var treePanel = new Ext.tree.TreePanel({
                      el: 'tree',
		      title : 'Process Tree',
		      loader: processTreeLoader,
		      animate: true,
		      autoScroll: true,
		      containerScroll: true
		});

	      var processTreeRoot = new Ext.tree.AsyncTreeNode({
		      text : 'Erlang VM',
		      draggable: false,
		      id : 'processTreeRoot',
		      expanded: true
		});

	      treePanel.setRootNode(processTreeRoot);
 
var myReader = new Ext.data.ArrayReader({}, [
{name: 'key'},
{name: 'value'}
					     ]);

var myJSONStore = new Ext.data.JsonStore({
	autoLoad: true,
	url: 'processinfo.yaws?group=&id=',
	root: 'pairs',
	fields: ['key', 'value']
    });


var myJSONStore = createStore("", "");

var tablePanel = new Ext.grid.GridPanel({
        el: 'table',
	store: myJSONStore,
	columns: [
{header: 'Key', width: 120, sortable: false, dataIndex: 'key'},
{header: 'Value', width: 555, sortable: false, dataIndex: 'value'}
		  ],
	viewConfig: {
	    forceFit: true
	},
	width: 675,
	title: 'Process Properties',
	autoExpandColumn: 'Value',
	frame: true
    });

treePanel.addListener( "click", function(node, e){
	if(node.leaf){
	    updateTable(node.parentNode.id, node.text, tablePanel);
	}
    });


treePanel.render();
tablePanel.render();

//    viewport = new Ext.Viewport({
//           layout:'border',
//           items:[treePanel,tablePanel]});
});
