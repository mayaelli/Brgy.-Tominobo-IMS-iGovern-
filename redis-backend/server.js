const express = require('express');
const redis = require('redis');
const cors = require('cors');
const bodyParser = require('body-parser');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(bodyParser.json());

// Connect to Redis
const client = redis.createClient({
  url: 'redis://@127.0.0.1:6379'  // Default Redis connection
});

client.connect().catch(console.error);

// CRUD Operations

app.post('/residents/bulk', async (req, res) => {
  const { residents } = req.body;

  if (!Array.isArray(residents) || residents.length === 0) {
    return res.status(400).json({ message: 'No residents provided' });
  }

  try {
    for (const resident of residents) {
      const key = `resident:${resident.id}`;
      await client.hSet(key, {
        firstname: resident.firstname,
        lastname: resident.lastname,
        dateofbirth: resident.dateofbirth,
        sex: resident.sex,
        householdID: resident.householdID,
        employmentStatus: resident.employmentStatus,
        occupation: resident.occupation,
        income: resident.income
      });
    }

    res.status(201).json({ message: 'Residents added successfully' });
  } catch (error) {
    console.error('Bulk save error:', error);
    res.status(500).json({ message: 'Failed to save bulk residents' });
  }
});


// Route to save resident data
app.post('/residents', async (req, res) => {
  const {id, firstname, lastname, dateofbirth, sex, 
    householdID, employmentStatus, occupation, income} = req.body;

  // Validate input fields
  if (!id || !firstname || !lastname || !dateofbirth || !sex ||
      !householdID ||
     !employmentStatus || !occupation || !income) {
    return res.status(400).json({ message: 'All fields are required' });
  }

  try {
    // Set resident data in Redis (using object syntax for Redis v4 and above)
    const residentData = { firstname, lastname, dateofbirth, sex,
      householdID, employmentStatus, occupation, income };


    // Save student data in Redis hash
    await client.hSet(`resident:${id}`, 'firstname', residentData.firstname);
    await client.hSet(`resident:${id}`, 'lastname', residentData.lastname);
    await client.hSet(`resident:${id}`, 'dateofbirth', residentData.dateofbirth);
    await client.hSet(`resident:${id}`, 'sex', residentData.sex);
    await client.hSet(`resident:${id}`, 'householdID', residentData.householdID);
    await client.hSet(`resident:${id}`, 'employmentStatus', residentData.employmentStatus);
    await client.hSet(`resident:${id}`, 'occupation', residentData.occupation);
    await client.hSet(`resident:${id}`, 'income', residentData.income);

    // Respond with success message
    res.status(201).json({ message: 'Saved successfully' });
  } catch (error) {
    console.error('Error saving resident:', error);
    res.status(500).json({ message: 'Failed to save resident' });
  }
});

// Read (R)
app.get('/residents/:id', async (req, res) => {
  const id = req.params.id;
  const resident = await client.hGetAll(`resident:${id}`);
  if (Object.keys(resident).length === 0) {
    return res.status(404).json({ message: 'Resident not found' });
  }
  res.json(resident);
});

// Read all residents
app.get('/residents', async (req, res) => {
  const keys = await client.keys('resident:*');
  const residents = await Promise.all(keys.map(async (key) => {
    return { id: key.split(':')[1], ...(await client.hGetAll(key)) };
  }));
  res.json(residents);
});

// Update (U)
app.put('/residents/:id', async (req, res) => {
  const id = req.params.id;
  const {firstname, lastname, dateofbirth, sex,
    householdID, employmentStatus, occupation, income} = req.body;

  if (!firstname && !lastname && !dateofbirth && !sex &&
     !householdID && !employmentStatus && !occupation && !income) {
    return res.status(400).json({ message: 'At least one field is required to update' });
  }

  try {
    const existingResident = await client.hGetAll(`resident:${id}`);
    if (Object.keys(existingResident).length === 0) {
      return res.status(404).json({ message: 'Resident not found' });
    }

    // Update resident data in Redis
    if (firstname) await client.hSet(`resident:${id}`, 'firstname', firstname);
    if (lastname) await client.hSet(`resident:${id}`, 'lastname', lastname);
    if (dateofbirth) await client.hSet(`resident:${id}`, 'dateofbirth', dateofbirth);
    if (sex) await client.hSet(`resident:${id}`, 'sex', sex);
    if (householdID) await client.hSet(`resident:${id}`, 'householdID', householdID);
    if (employmentStatus) await client.hSet(`resident:${id}`, 'employmentStatus', employmentStatus);
    if (occupation) await client.hSet(`resident:${id}`, 'occupation', occupation);
    if (income) await client.hSet(`resident:${id}`, 'income', income);
  
    res.status(200).json({ message: 'Resident updated successfully' });
  } catch (error) {
    console.error('Error updating resident:', error);
    res.status(500).json({ message: 'Failed to update resident' });
  }
});

// Delete (D)
app.delete('/residents/:id', async (req, res) => {
  const id = req.params.id;
  await client.del(`resident:${id}`);
  res.status(200).json({ message: 'Resident deleted successfully' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});