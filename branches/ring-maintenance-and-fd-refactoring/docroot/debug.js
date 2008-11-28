function updateTable(id, group, tablePanel){
    tablePanel.reconfigure(createStore(id, group), 
			   new Ext.grid.ColumnModel([
		{header: 'Key', width: 120, sortable: false, dataIndex: 'key'},
		    {header: 'Value', width: 90, sortable: false, dataIndex: 'value'}
		    ]));
    tablePanel.render();
}

function createStore(P0, P1){
    return new Ext.data.JsonStore({
	    autoLoad: true,
		url: 'processinfo.yaws?p0=' + P0 + '&p1=' + P1,
		root: 'pairs',
		fields: ['key', 'value']
		});
}

Ext.onReady(function() {
	// Note: For the purposes of following along with the tutorial, all 
	// new code should be placed inside this method.  Delete the following
	// line after you have verified that Ext is installed correctly.

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
		      autoScoll: true,
		      containerScroll: true
		});

	      var processTreeRoot = new Ext.tree.AsyncTreeNode({
		      text : 'Erlang VM',
		      draggable: false,
		      id : 'processTreeRoot'
		});

	      treePanel.setRootNode(processTreeRoot);

var myData = [
		['Apple',29.89,0.24,0.81],
		['Ext',83.81,0.28,0.34],
		['Google',71.72,0.02,0.03],
		['Microsoft',52.55,0.01],
		['Yahoo!',29.01]
	];
 
var myReader = new Ext.data.ArrayReader({}, [
{name: 'key'},
{name: 'value'}
					     ]);

var myJSONStore = new Ext.data.JsonStore({
	autoLoad: true,
	url: 'processinfo.yaws?p0=&p1=',
	root: 'pairs',
	fields: ['key', 'value']
    });


var myJSONStore = createStore("", "");

var tablePanel = new Ext.grid.GridPanel({
        el: 'table',
	store: myJSONStore,
	columns: [
{header: 'Key', width: 120, sortable: false, dataIndex: 'key'},
{header: 'Value', width: 90, sortable: false, dataIndex: 'value'}
		  ],
	viewConfig: {
	    forceFit: true
	},
	width: 500,
	title: 'Process Properties',
	autoExpandColumn: 'Value',
	frame: true
    });

treePanel.addListener( "dblclick", function(node, e){
	if(node.leaf){
	    updateTable(node.parentNode.id, node.id, tablePanel);
	}
    });


treePanel.render();
tablePanel.render();

//    viewport = new Ext.Viewport({
//           layout:'border',
//           items:[treePanel,tablePanel]});
});
