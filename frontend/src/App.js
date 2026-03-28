import React, { useState, useRef } from 'react';
import GraphView from './GraphView';
import ChatPanel from './ChatPanel';
import './App.css';

function App() {
  const [highlightedNodes, setHighlightedNodes] = useState([]);
  const graphRef = useRef(null);

  const handleHighlight = (ids) => {
    setHighlightedNodes(ids);
    if (graphRef.current) {
      graphRef.current.focusNodes(ids);
    }
  };

  return (
    <div className="app-container">
      <div className="header">
        <h1>Order-to-Cash Explorer</h1>
        <span>Sales Order → Delivery → Billing → Journal Entry → Payment</span>
      </div>
      <div className="main-content">
        <div className="graph-panel">
          <GraphView ref={graphRef} highlightedNodes={highlightedNodes} />
        </div>
        <div className="chat-panel">
          <ChatPanel onHighlight={handleHighlight} />
        </div>
      </div>
    </div>
  );
}

export default App;