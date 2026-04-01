import React, { useState, useRef, useEffect } from 'react';
import axios from 'axios';

const API = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const SUGGESTIONS = [
  "Which products are associated with the highest number of billing documents?",
  "Which sales orders were delivered but never billed?",
  "Which customers have the most incomplete order flows?",
  "Trace the full flow of billing document 90504204",
];

function renderAnswer(text, results) {
  if (!text) return null;
  
  const lines = text.split('\n').filter(line => line.trim());
  const stepKeywords = ['Sales Order:', 'Delivery:', 'Billing:', 'Journal Entry:', 'Payment:'];
  
  return (
    <div>
      {lines.map((line, i) => {
        // Warning lines for cancelled documents
        if (line.startsWith('Note:')) {
          return (
            <div key={i} style={{
              background: '#fef3c7',
              border: '1px solid #f59e0b',
              borderRadius: 6,
              padding: '8px 12px',
              fontSize: 12,
              color: '#92400e',
              marginTop: 8,
              marginBottom: 0,
              fontWeight: 500,
            }}>
              {line}
            </div>
          );
        }
        
        // Step labels for trace queries
        if (stepKeywords.some(kw => line.startsWith(kw))) {
          const colonIndex = line.indexOf(':');
          const label = line.substring(0, colonIndex);
          const rest = line.substring(colonIndex + 1).trim();
          
          return (
            <div key={i} style={{
              display: 'flex',
              gap: 12,
              paddingTop: i > 0 ? 6 : 0,
              paddingBottom: 6,
              borderBottom: '1px solid #f3f4f6',
              fontSize: 12,
            }}>
              <span style={{
                color: '#64748b',
                fontWeight: 600,
                minWidth: 110,
                flexShrink: 0,
              }}>
                {label}
              </span>
              <span style={{ color: '#111827', flex: 1 }}>
                {rest}
              </span>
            </div>
          );
        }
        
        // Regular lines
        return (
          <div key={i} style={{
            fontSize: 12,
            color: '#111827',
            lineHeight: 1.7,
            paddingBottom: 4,
          }}>
            {line}
          </div>
        );
      })}
      
      {/* Results table for >3 rows */}
      {results && results.length > 3 && (
        <details style={{ marginTop: 10 }}>
          <summary style={{
            fontSize: 11,
            color: '#6b7280',
            cursor: 'pointer',
            userSelect: 'none',
            paddingTop: 6,
            fontWeight: 500,
          }}>
            Show all {results.length} results
          </summary>
          <div style={{
            marginTop: 8,
            borderRadius: 6,
            overflow: 'hidden',
            fontSize: 11,
            border: '1px solid #e5e7eb',
          }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ background: '#f9fafb', borderBottom: '1px solid #e5e7eb' }}>
                  {Object.keys(results[0] || {}).slice(0, 5).map(key => (
                    <th key={key} style={{
                      color: '#6b7280',
                      fontWeight: 600,
                      padding: '8px 10px',
                      textAlign: 'left',
                      borderRight: '1px solid #e5e7eb',
                    }}>
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {results.slice(0, 5).map((row, idx) => (
                  <tr key={idx} style={{
                    background: idx % 2 === 0 ? '#fff' : '#f9fafb',
                    borderBottom: '1px solid #e5e7eb',
                  }}>
                    {Object.keys(results[0] || {}).slice(0, 5).map(key => (
                      <td key={key} style={{
                        padding: '8px 10px',
                        color: '#111827',
                        borderRight: '1px solid #e5e7eb',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        maxWidth: 200,
                      }}>
                        {String(row[key] || '').substring(0, 50)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}
    </div>
  );
}

export default function ChatPanel({ onHighlight }) {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      text: 'Hello! Ask me anything about your Order-to-Cash data. I can trace order flows, find billing issues, analyze products, and more.',
      sql: null,
      results: null,
    }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendMessage = async (question) => {
    const q = question || input.trim();
    if (!q || loading) return;

    setInput('');
    setMessages(prev => [...prev, { role: 'user', text: q }]);
    setLoading(true);

    try {
      const res = await axios.post(`${API}/query`, { question: q });
      const { answer, sql, results } = res.data;

      setMessages(prev => [...prev, {
        role: 'assistant',
        text: answer,
        sql: sql,
        results: results || [],
        resultCount: results?.length || 0,
      }]);

      if (results?.length) {
        // Extract all string values from results to match against node IDs
        const ids = results.slice(0, 10).flatMap(r =>
          Object.values(r).map(String).filter(v => v.length > 3)
        );
        onHighlight(ids);
      }

    } catch (err) {
      console.error('Failed to send query:', err);
      setMessages(prev => [...prev, {
        role: 'assistant',
        text: `Something went wrong. Make sure the backend is running at ${API}\n\nError: ${err.message}`,
        sql: null,
        results: null,
      }]);
    }

    setLoading(false);
  };

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      height: '100%',
      background: '#ffffff',
    }}>

      {/* Header */}
      <div style={{
        padding: '16px 20px',
        borderBottom: '1px solid #e5e7eb',
        flexShrink: 0,
      }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: '#111827' }}>
          Chat with Graph
        </div>
        <div style={{ fontSize: 12, color: '#6b7280', marginTop: 2 }}>
          Order to Cash
        </div>
      </div>

      {/* Messages */}
      <div style={{
        flex: 1,
        overflowY: 'auto',
        padding: '16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
        minHeight: 0,
      }}>

        {messages.map((msg, i) => (
          <div key={i} style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: msg.role === 'user' ? 'flex-end' : 'flex-start',
          }}>

            {/* Role label */}
            <div style={{
              fontSize: 11,
              color: '#9ca3af',
              marginBottom: 4,
              fontWeight: 500,
            }}>
              {msg.role === 'user' ? 'You' : 'Graph Agent'}
            </div>

            {/* Bubble */}
            <div style={{
              maxWidth: '88%',
              background: msg.role === 'user' ? '#111827' : '#f9fafb',
              border: msg.role === 'user' ? 'none' : '1px solid #e5e7eb',
              borderRadius: msg.role === 'user' ? '12px 12px 2px 12px' : '12px 12px 12px 2px',
              padding: '10px 14px',
              fontSize: '13px',
              lineHeight: '1.8',
              color: msg.role === 'user' ? '#ffffff' : '#111827',
              whiteSpace: 'normal',
            }}>
              {msg.role === 'assistant' ? renderAnswer(msg.text, msg.results) : msg.text}
            </div>

            {/* SQL toggle */}
            {msg.sql && (
              <details style={{ marginTop: 6, maxWidth: '88%', width: '88%' }}>
                <summary style={{
                  fontSize: 11,
                  color: '#6b7280',
                  cursor: 'pointer',
                  userSelect: 'none',
                  padding: '4px 0',
                }}>
                  View SQL · {msg.resultCount} rows returned
                </summary>
                <pre style={{
                  background: '#f3f4f6',
                  border: '1px solid #e5e7eb',
                  borderRadius: 6,
                  padding: '10px 12px',
                  fontSize: 10,
                  color: '#059669',
                  overflowX: 'auto',
                  marginTop: 4,
                  lineHeight: 1.6,
                }}>
                  {msg.sql}
                </pre>
              </details>
            )}
          </div>
        ))}

        {/* Thinking indicator */}
        {loading && (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start' }}>
            <div style={{ fontSize: 11, color: '#9ca3af', marginBottom: 4, fontWeight: 500 }}>
              Graph Agent
            </div>
            <div style={{
              background: '#f9fafb',
              border: '1px solid #e5e7eb',
              borderRadius: '12px 12px 12px 2px',
              padding: '10px 14px',
              fontSize: 13,
              color: '#6b7280',
              display: 'flex',
              alignItems: 'center',
              gap: 6,
            }}>
              <span style={{ animation: 'pulse 1.5s infinite' }}>●</span>
              <span>Analyzing data...</span>
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Suggestions */}
      {messages.length <= 1 && (
        <div style={{
          padding: '0 16px 12px',
          display: 'flex',
          flexDirection: 'column',
          gap: 6,
          flexShrink: 0,
        }}>
          <div style={{ fontSize: 11, color: '#9ca3af', marginBottom: 2, fontWeight: 500 }}>
            Try asking
          </div>
          {SUGGESTIONS.map((s, i) => (
            <button key={i} onClick={() => sendMessage(s)} style={{
              background: '#f9fafb',
              border: '1px solid #e5e7eb',
              borderRadius: 8,
              padding: '8px 12px',
              color: '#374151',
              fontSize: 12,
              textAlign: 'left',
              cursor: 'pointer',
              lineHeight: 1.4,
              transition: 'background 0.15s',
            }}
            onMouseEnter={e => e.target.style.background = '#f3f4f6'}
            onMouseLeave={e => e.target.style.background = '#f9fafb'}
            >
              {s}
            </button>
          ))}
        </div>
      )}

      {/* Status indicator */}
      <div style={{
        padding: '8px 16px',
        borderTop: '1px solid #f3f4f6',
        display: 'flex',
        alignItems: 'center',
        gap: 6,
        flexShrink: 0,
      }}>
        <div style={{
          width: 7,
          height: 7,
          borderRadius: '50%',
          background: loading ? '#f59e0b' : '#10b981',
        }} />
        <span style={{ fontSize: 11, color: '#9ca3af' }}>
          {loading ? 'Processing query...' : 'Graph Agent is ready'}
        </span>
      </div>

      {/* Input */}
      <div style={{
        padding: '12px 16px',
        borderTop: '1px solid #e5e7eb',
        display: 'flex',
        gap: 8,
        flexShrink: 0,
        background: '#ffffff',
      }}>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && sendMessage()}
          placeholder="Analyze anything..."
          style={{
            flex: 1,
            background: '#f9fafb',
            border: '1px solid #e5e7eb',
            borderRadius: 8,
            padding: '10px 14px',
            color: '#111827',
            fontSize: 13,
            outline: 'none',
          }}
        />
        <button
          onClick={() => sendMessage()}
          disabled={loading}
          style={{
            background: '#111827',
            border: 'none',
            borderRadius: 8,
            padding: '10px 18px',
            color: '#fff',
            fontWeight: 600,
            fontSize: 13,
            cursor: loading ? 'not-allowed' : 'pointer',
            opacity: loading ? 0.5 : 1,
            transition: 'opacity 0.15s',
          }}
        >
          Send
        </button>
      </div>
    </div>
  );
}