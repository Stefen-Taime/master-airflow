import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { FaMoneyBillAlt, FaBuilding, FaCity } from 'react-icons/fa';


const HomePage = () => {
  const [data, setData] = useState({ highest_price: {}, lowest_price: {} });

  useEffect(() => {
    const fetchData = async () => {
      const result = await axios('http://localhost:8000/prices');
      setData(result.data);
    };

    fetchData();
  }, []);

  return (
    
    <div className="container">
      <div className="price-card">
        <h2>Highest Price</h2>
        <p><FaMoneyBillAlt /> Price: {data.highest_price.price} $</p>
        <p><FaBuilding /> Station: {data.highest_price.station}</p>
        <p><FaCity /> City: {data.highest_price.city}</p>
      </div>
      <div className="price-card">
        <h2>Lowest Price</h2>
        <p><FaMoneyBillAlt /> Price: {data.lowest_price.price} $</p>
        <p><FaBuilding /> Station: {data.lowest_price.station}</p>
        <p><FaCity /> City: {data.lowest_price.city}</p>
      </div>
    </div>
  );
}

export default HomePage;
