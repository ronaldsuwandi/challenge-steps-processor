import {Kafka} from "kafkajs";
import { addHours } from "date-fns";

const kafka = new Kafka({brokers: ['localhost:9092']});
const producer = kafka.producer();

// Constants for user types
const USER_TYPES = {
    HIGH: 'HIGH',
    MEDIUM: 'MEDIUM',
    LOW: 'LOW',
    LOW_TO_HIGH: 'LOW_TO_HIGH'
};

// Parse command-line arguments
const [userType, userCountArg] = process.argv.slice(2);
const userCount = parseInt(userCountArg, 10);

// Validate arguments
if (!USER_TYPES[userType]) {
    console.error(`Invalid user type specified. Please choose from: ${Object.keys(USER_TYPES).join(', ')}`);
    process.exit(1);
}
if (isNaN(userCount) || userCount <= 0) {
    console.error('Please specify a valid number of users.');
    process.exit(1);
}

// Generate users with specified naming convention and count
const users = Array.from({length: userCount}, (_, index) => ({
    userId: `${userType}-user-${index + 1}`,
    type: userType,
    baseSteps: 0,
    isAppleDevice: index % 2 == 0, // for simplicity, even number user uses apple
}));

// Helper functions for step counts based on user type
function getHourlySteps(user) {
    switch (user.type) {
        case USER_TYPES.HIGH:
            return 130 + Math.floor(Math.random() * 50);  // High range hourly steps (>3k/day)
        case USER_TYPES.MEDIUM:
            return 50 + Math.floor(Math.random() * 50);  // Medium range hourly steps (1000-3000/day)
        case USER_TYPES.LOW:
            return Math.floor(Math.random() * 30);         // Low range hourly steps (<1000/day)
        case USER_TYPES.LOW_TO_HIGH:
            user.baseSteps = Math.min(user.baseSteps + 50, 500); // Gradually increase hourly steps
            return user.baseSteps;
        default:
            return 0;
    }
}

const date = new Date();

// Generate data in Apple or Google format, flipping device and location data
function generateData(user, count) {
    const steps = getHourlySteps(user);
    const isApple = user.isAppleDevice;
    const hasLocation = Math.random() < 0.5;   // Flip location data randomly

    let ts = addHours(date, count).toISOString();
    const baseData = {
        Id: user.userId,
        ts: ts,
        steps: steps,
        location: hasLocation ? {lat: 1.3521, lon: 103.8198, pincode: 333001} : undefined
    };

    if (isApple) {
        return {
            UserFirstName: baseData.Id,
            UserLastName: "Doe",
            UserId: baseData.Id,
            UserGender: "male",
            UserHeight: 175,
            UserWeight: 70,
            PhysicalActivity: {
                Dance_minutes: 100,
                Yoga_minutes: 100,
                Sleep_hours: 3,
                walking_steps: baseData.steps
            },
            Geo_lat: baseData.location ? baseData.location.lat : null,
            Geo_lon: baseData.location ? baseData.location.lon : null,
            pincode: baseData.location ? baseData.location.pincode : null,
            event_timestamp: baseData.ts
        };
    } else {
        return {
            Person: {
                FirstName: baseData.Id,
                LastName: "Doe",
                Id: baseData.Id,
                Gender: "male",
                Height: 175,
                Weight: 70
            },
            Activity: {
                Dance_minutes: 100,
                Yoga_minutes: 100,
                Sleep_hours: 3,
                walking_steps: baseData.steps
            },
            Location: baseData.location || undefined,
            ts: baseData.ts
        };
    }
}

// Main function to produce scenario-based data
async function produceScenarioData() {
    await producer.connect();

    let count = 0;
    setInterval(async () => {
        // Randomly select a user from the specified type and generate data
        for (const user of users) {
            const messageData = generateData(user, count);

            const message = {
                key: user.userId,
                value: JSON.stringify(messageData)
            };

            let topic;
            if (user.isAppleDevice) {
                topic = 'user-activity-ios';
            } else {
                topic = 'user-activity-android';
            }
            await producer.send({
                topic: topic,
                messages: [message]
            });

            console.log(`[Produced] ${user.userId} - ${user.isAppleDevice ? 'Apple' : 'Android'} format:`, messageData);
        }
        count++;
    }, 1000);
}

// Run the producer with specified arguments
produceScenarioData().catch(console.error);
