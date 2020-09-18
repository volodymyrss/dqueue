import React, { useState, useEffect } from 'react';
import { useTable } from 'react-table'
import logo from './logo.svg';
import './App.css';

function App() {
    const [tasks, setTasks] = useState(0);

    useEffect(() => {
        fetch('/tasks').then(res => res.json()).then(data => {
                 setTasks(data.tasks)
              });
         }, []); 
    
    const [task_data, setTaskData] = useState(0);
    
    useEffect(() => {
        fetch('/tasks').then(res => res.json()).then(data => {
                 setTaskData(Array.from(data.tasks).map(task => ({
                     col1: task.key,
                     col2: task.created,
                 })), [])
              });
         }); 

 const data = React.useMemo(
   () => task_data, []
 )

 const xdata = React.useMemo(
   () => [
     {
       col1: 'Hello',
       col2: 'World',
     },
     {
       col1: 'react-table',
       col2: 'rocks',
     },
     {
       col1: 'whatever',
       col2: 'you want',
     },
   ],
   []
 )


 const columns = React.useMemo(
   () => [
     {
       Header: 'Column 1',
       accessor: 'col1', // accessor is the "key" in the data
     },
     {
       Header: 'Column 2',
       accessor: 'col2',
     },
   ],
   []
 )

 const {
     getTableProps,
     getTableBodyProps,
     headerGroups,
     rows,
     prepareRow,
   } = useTable({ columns, task_data })
                    //            <td>{task.queue}</td>
                    //            <td>{task.state}</td>
                    //            <td>{task.worker_id}</td>
                   //             <td>{task.modified}</td>

  return (
    <div className="App">
      <header className="App-header">
      </header>

        <table>
          { Array.from(tasks).map((task) => <tr>
                                <td>{task.key}</td>
                                <td>{task.created}</td>
                            </tr> )}
        </table>


    <table {...getTableProps()} style={{ border: 'solid 1px blue' }}>
       <thead>
         {headerGroups.map(headerGroup => (
           <tr {...headerGroup.getHeaderGroupProps()}>
             {headerGroup.headers.map(column => (
               <th
                 {...column.getHeaderProps()}
                 style={{
                   borderBottom: 'solid 3px red',
                   background: 'aliceblue',
                   color: 'black',
                   fontWeight: 'bold',
                 }}
               >
                 {column.render('Header')}
               </th>
             ))}
           </tr>
         ))}
       </thead>
       <tbody {...getTableBodyProps()}>
         {rows.map(row => {
           prepareRow(row)
           return (
             <tr {...row.getRowProps()}>
               {row.cells.map(cell => {
                 return (
                   <td
                     {...cell.getCellProps()}
                     style={{
                       padding: '10px',
                       border: 'solid 1px gray',
                       background: 'papayawhip',
                     }}
                   >
                     {cell.render('Cell')}
                   </td>
                 )
               })}
             </tr>
           )
         })}
       </tbody>
     </table>

    </div>
   )

 }




export default App;
