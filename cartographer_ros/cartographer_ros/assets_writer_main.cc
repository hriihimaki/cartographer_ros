/*
 * Copyright 2016 The Cartographer Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "cartographer/common/configuration_file_resolver.h"
#include "cartographer/common/make_unique.h"
#include "cartographer/common/math.h"
#include "cartographer/io/file_writer.h"
#include "cartographer/io/points_processor.h"
#include "cartographer/io/points_processor_pipeline_builder.h"
#include "cartographer/io/proto_stream.h"
#include "cartographer/mapping/proto/pose_graph.pb.h"
#include "cartographer/sensor/point_cloud.h"
#include "cartographer/sensor/range_data.h"
#include "cartographer/transform/transform_interpolation_buffer.h"
#include "cartographer_ros/msg_conversion.h"
#include "cartographer_ros/ros_map_writing_points_processor.h"
#include "cartographer_ros/split_string.h"
#include "cartographer_ros/time_conversion.h"
#include "cartographer_ros/urdf_reader.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "ros/ros.h"
#include "ros/time.h"
#include "rosbag/bag.h"
#include "rosbag/view.h"
#include "tf2_eigen/tf2_eigen.h"
#include "tf2_msgs/TFMessage.h"
#include "tf2_ros/buffer.h"
#include "urdf/model.h"
#include "pcl/point_cloud.h"
#include "pcl/point_types.h"
#include "pcl_conversions/pcl_conversions.h"
//las writer
#include "LAStools/inc/laswriter.hpp"
//#include "cartographer/io/color.h"

DEFINE_string(configuration_directory, "",
              "First directory in which configuration files are searched, "
              "second is always the Cartographer installation to allow "
              "including files from there.");
DEFINE_string(configuration_basename, "",
              "Basename, i.e. not containing any directory prefix, of the "
              "configuration file.");
DEFINE_string(
    urdf_filename, "",
    "URDF file that contains static links for your sensor configuration.");
DEFINE_string(bag_filenames, "",
              "Bags to process, must be in the same order as the trajectories "
              "in 'pose_graph_filename'.");
DEFINE_string(pose_graph_filename, "",
              "Proto stream file containing the pose graph.");
DEFINE_bool(use_bag_transforms, true,
            "Whether to read and use the transforms from the bag.");
DEFINE_string(output_file_prefix, "",
              "Will be prefixed to all output file names and can be used to "
              "define the output directory. If empty, the first bag filename "
              "will be used.");

std::string trajectoryName = "trajectory.txt";

namespace cartographer_ros {
namespace {

constexpr char kTfStaticTopic[] = "/tf_static";
namespace carto = ::cartographer;

template <typename T>
std::unique_ptr<carto::io::PointsBatch> HandleMessage(
    const T& message, const std::string& tracking_frame,
    const tf2_ros::Buffer& tf_buffer,
    const carto::transform::TransformInterpolationBuffer&
        transform_interpolation_buffer) {
  const carto::common::Time start_time = FromRos(message.header.stamp);

  auto points_batch = carto::common::make_unique<carto::io::PointsBatch>();
  points_batch->start_time = start_time;
  points_batch->frame_id = message.header.frame_id;

  carto::sensor::PointCloudWithIntensities point_cloud =
      ToPointCloudWithIntensities(message);

  CHECK_EQ(point_cloud.intensities.size(), point_cloud.points.size());
  CHECK_EQ(point_cloud.rings.size(), point_cloud.points.size());

  for (size_t i = 0; i < point_cloud.points.size(); ++i) {
    const carto::common::Time time =
        start_time + carto::common::FromSeconds(point_cloud.points[i][3]);
    if (!transform_interpolation_buffer.Has(time)) {
      continue;
    }
    const carto::transform::Rigid3d tracking_to_map =
        transform_interpolation_buffer.Lookup(time);
    const carto::transform::Rigid3d sensor_to_tracking =
        ToRigid3d(tf_buffer.lookupTransform(
            tracking_frame, message.header.frame_id, ToRos(time)));
    const carto::transform::Rigid3f sensor_to_map =
        (tracking_to_map * sensor_to_tracking).cast<float>();
    points_batch->points.push_back(sensor_to_map *
                                   point_cloud.points[i].head<3>());
    //intensities
    points_batch->intensities.push_back(point_cloud.intensities[i]);
    //rings
    points_batch->rings.push_back((int8_t)point_cloud.rings[i]);
    //echoes
    points_batch->echoes.push_back(point_cloud.echoes[i]);
    //color
    //cartographer::io::FloatColor c = {{(float)point_cloud.reds[i]*(float)256, (float)point_cloud.greens[i]*(float)256, (float)point_cloud.blues[i]*(float)256}};
    cartographer::io::FloatColor c = {{(float)point_cloud.reds[i], (float)point_cloud.greens[i], (float)point_cloud.blues[i]}};
    points_batch->colors.push_back(c);
    //unix time
    points_batch->start_time_unix = message.header.stamp.toSec();
    // We use the last transform for the origin, which is approximately correct.
    points_batch->origin = sensor_to_map * Eigen::Vector3f::Zero();
    
  }

  //rotation & translation for writing trajectory
  if (point_cloud.points.size() > 0){
  	const carto::common::Time time_start = start_time + carto::common::FromSeconds(point_cloud.points[size_t(0)][3]);
  	if (transform_interpolation_buffer.Has(time_start)) {
  		const carto::transform::Rigid3d tracking_to_map_start = transform_interpolation_buffer.Lookup(time_start);
  		const carto::transform::Rigid3d sensor_to_tracking_start = ToRigid3d(tf_buffer.lookupTransform(tracking_frame, message.header.frame_id, ToRos(time_start)));
  		const carto::transform::Rigid3f sensor_to_map_start = (tracking_to_map_start * sensor_to_tracking_start).cast<float>();
  
  		//write trajectory
  		std::ofstream outfile;
  		outfile.open(trajectoryName, std::ios_base::app);
  		outfile << message.header.stamp << " " << std::setprecision(10) << sensor_to_map_start.translation()(0,0) << " " << sensor_to_map_start.translation()(1,0) << " " << sensor_to_map_start.translation()(2,0) << " " << sensor_to_map_start.rotation().x() << " " << sensor_to_map_start.rotation().y() << " " << sensor_to_map_start.rotation().z() << " " << sensor_to_map_start.rotation().w() << "\n";
		outfile.close();
  	}
  }
  
  if (points_batch->points.empty()) {
    return nullptr;
  }
  return points_batch;
}

void Run(const std::string& pose_graph_filename,
         const std::vector<std::string>& bag_filenames,
         const std::string& configuration_directory,
         const std::string& configuration_basename,
         const std::string& urdf_filename,
         const std::string& output_file_prefix) {
  auto file_resolver =
      carto::common::make_unique<carto::common::ConfigurationFileResolver>(
          std::vector<std::string>{configuration_directory});
  const std::string code =
      file_resolver->GetFileContentOrDie(configuration_basename);
  carto::common::LuaParameterDictionary lua_parameter_dictionary(
      code, std::move(file_resolver));

  carto::io::ProtoStreamReader reader(pose_graph_filename);
  carto::mapping::proto::PoseGraph pose_graph_proto;
  CHECK(reader.ReadProto(&pose_graph_proto));
  CHECK_EQ(pose_graph_proto.trajectory_size(), bag_filenames.size())
      << "Pose graphs contains " << pose_graph_proto.trajectory_size()
      << " trajectories while " << bag_filenames.size()
      << " bags were provided. This tool requires one bag for each "
         "trajectory in the same order as the correponding trajectories in the "
         "pose graph proto.";

  const std::string file_prefix = !output_file_prefix.empty()
                                      ? output_file_prefix
                                      : bag_filenames.front() + "_";
  const auto file_writer_factory = [file_prefix](const std::string& filename) {
    return carto::common::make_unique<carto::io::StreamFileWriter>(file_prefix +
                                                                   filename);
  };

  // This vector must outlive the pipeline.
  std::vector<::cartographer::mapping::proto::Trajectory> all_trajectories(
      pose_graph_proto.trajectory().begin(),
      pose_graph_proto.trajectory().end());

  carto::io::PointsProcessorPipelineBuilder builder;
  carto::io::RegisterBuiltInPointsProcessors(all_trajectories,
                                             file_writer_factory, &builder);
  builder.Register(
      RosMapWritingPointsProcessor::kConfigurationFileActionName,
      [file_writer_factory](
          ::cartographer::common::LuaParameterDictionary* const dictionary,
          ::cartographer::io::PointsProcessor* const next)
          -> std::unique_ptr<::cartographer::io::PointsProcessor> {
        return RosMapWritingPointsProcessor::FromDictionary(file_writer_factory,
                                                            dictionary, next);
      });

  std::vector<std::unique_ptr<carto::io::PointsProcessor>> pipeline =
      builder.CreatePipeline(
          lua_parameter_dictionary.GetDictionary("pipeline").get());
  
  //check if .las export is chosen
  std::size_t las_found = code.find(std::string("write_las"));
  
  if(las_found!=std::string::npos){
	pipeline.back()->outputName = bag_filenames.front() + std::string(".las");
  	std::cout << ".las export is chosen " << las_found << ", output name: " << pipeline.back()->outputName << std::endl; 
  }
  
  //.las filename
  //pipeline.back()->outputName = bag_filenames.front() + std::string(".las");

  const std::string tracking_frame =
      lua_parameter_dictionary.GetString("tracking_frame");
  do {
    for (size_t trajectory_id = 0; trajectory_id < bag_filenames.size();
         ++trajectory_id) {
      const carto::mapping::proto::Trajectory& trajectory_proto =
          pose_graph_proto.trajectory(trajectory_id);
      const std::string& bag_filename = bag_filenames[trajectory_id];
      LOG(INFO) << "Processing " << bag_filename << "...";
      if (trajectory_proto.node_size() == 0) {
        continue;
      }
      tf2_ros::Buffer tf_buffer;
      if (!urdf_filename.empty()) {
        ReadStaticTransformsFromUrdf(urdf_filename, &tf_buffer);
      }

      const carto::transform::TransformInterpolationBuffer
          transform_interpolation_buffer(trajectory_proto);
      rosbag::Bag bag;
      bag.open(bag_filename, rosbag::bagmode::Read);
      rosbag::View view(bag);
      const ::ros::Time begin_time = view.getBeginTime();
      const double duration_in_seconds =
          (view.getEndTime() - begin_time).toSec();

      // We need to keep 'tf_buffer' small because it becomes very inefficient
      // otherwise. We make sure that tf_messages are published before any data
      // messages, so that tf lookups always work.
      std::deque<rosbag::MessageInstance> delayed_messages;
      // We publish tf messages one second earlier than other messages. Under
      // the assumption of higher frequency tf this should ensure that tf can
      // always interpolate.
      const ::ros::Duration kDelay(1.);
      
      //------for las export------
      bool usingLas = false;
      if(pipeline.back()->outputName.compare(std::string("NaN")) != 0){
	usingLas = true;
      }
      //open .las file
      LASwriteOpener laswriteopener;
      laswriteopener.set_file_name(pipeline.back()->outputName.c_str());
      // init header
      LASheader lasheader;
      lasheader.x_scale_factor = 0.001;
      lasheader.y_scale_factor = 0.001;
      lasheader.z_scale_factor = 0.001;
      lasheader.x_offset = 0.0;
      lasheader.y_offset = 0.0;
      lasheader.z_offset = 0.0;
      lasheader.point_data_format = 3;
      lasheader.point_data_record_length = 34;
      // init point 
      LASpoint laspoint;
      laspoint.init(&lasheader, lasheader.point_data_format, lasheader.point_data_record_length, 0);
      //open writing
      LASwriter* laswriter = laswriteopener.open(&lasheader);
      //-----------------------------

      for (const rosbag::MessageInstance& message : view) {
        if (FLAGS_use_bag_transforms && message.isType<tf2_msgs::TFMessage>()) {
          auto tf_message = message.instantiate<tf2_msgs::TFMessage>();
          for (const auto& transform : tf_message->transforms) {
            try {
              tf_buffer.setTransform(transform, "unused_authority",
                                     message.getTopic() == kTfStaticTopic);
            } catch (const tf2::TransformException& ex) {
              LOG(WARNING) << ex.what();
            }
          }
        }

        while (!delayed_messages.empty() && delayed_messages.front().getTime() <
                                                message.getTime() - kDelay) {
          const rosbag::MessageInstance& delayed_message =
              delayed_messages.front();

          std::unique_ptr<carto::io::PointsBatch> points_batch;
          if (delayed_message.isType<sensor_msgs::PointCloud2>()) {
            points_batch = HandleMessage(
                *delayed_message.instantiate<sensor_msgs::PointCloud2>(),
                tracking_frame, tf_buffer, transform_interpolation_buffer);
          } else if (delayed_message
                         .isType<sensor_msgs::MultiEchoLaserScan>()) {
            points_batch = HandleMessage(
                *delayed_message.instantiate<sensor_msgs::MultiEchoLaserScan>(),
                tracking_frame, tf_buffer, transform_interpolation_buffer);
          } else if (delayed_message.isType<sensor_msgs::LaserScan>()) {
            points_batch = HandleMessage(
                *delayed_message.instantiate<sensor_msgs::LaserScan>(),
                tracking_frame, tf_buffer, transform_interpolation_buffer);
          }
          if (points_batch != nullptr) {
	    //export las
	  if(usingLas){
		bool has_colors_ = !points_batch->colors.empty();
		//std::cout << has_colors_ << std::endl;
		for(size_t ii = 0; ii<points_batch->points.size(); ii++){
			// populate the point
			//coordinates
			double xdouble = points_batch->points[ii][0];
			I32 x;
			if (xdouble >= lasheader.x_scale_factor){
				x=(I32)((xdouble-lasheader.x_scale_factor)/lasheader.x_scale_factor+0.5);
			}else{
				x=(I32)((xdouble-lasheader.x_scale_factor)/lasheader.x_scale_factor-0.5);
			}
    			laspoint.set_X(x);

			double ydouble = points_batch->points[ii][1];
			I32 y;
			if (ydouble >= lasheader.y_scale_factor){
				y=(I32)((ydouble-lasheader.y_scale_factor)/lasheader.y_scale_factor+0.5);
			}else{
				y=(I32)((ydouble-lasheader.y_scale_factor)/lasheader.y_scale_factor-0.5);
			}
    			laspoint.set_Y(y);

			double zdouble = points_batch->points[ii][2];
			I32 z;
			if (zdouble >= lasheader.z_scale_factor){
				z=(I32)((zdouble-lasheader.z_scale_factor)/lasheader.z_scale_factor+0.5);
			}else{
				z=(I32)((zdouble-lasheader.z_scale_factor)/lasheader.z_scale_factor-0.5);
			}
    			laspoint.set_Z(z);
			//intensity
    			laspoint.set_intensity((U16)points_batch->intensities[ii]);
			//std::cout << "points_batch->intensities[ii] "  << " " << points_batch->intensities[ii] << std::endl;
			//time
    			//laspoint.set_gps_time((F64)ToUniversalDouble(points_batch->start_time));
			laspoint.set_gps_time((F64)points_batch->start_time_unix);
			//ring as scan direction
			laspoint.set_scan_angle_rank((I8)points_batch->rings[ii]);
			//echo
			laspoint.set_return_number((U8)points_batch->echoes[ii]);
			//rgb + nir
			if(has_colors_){
				//std::cout << points_batch->colors[ii][0] << " " << points_batch->colors[ii][0] << std::endl;
				//U16 rgb[4] = { (U16)points_batch->colors[ii][0] * (U16)256, (U16)points_batch->colors[ii][1] * (U16)256, (U16)points_batch->colors[ii][2] * (U16)256, (U16)0 };
				U16 rgb[4] = { (U16)((double)points_batch->colors[ii][0]*256.0), (U16)((double)points_batch->colors[ii][1]*256.0), (U16)((double)points_batch->colors[ii][2]*256.0), (U16)0 };
				laspoint.set_RGB(rgb);
			}

			
			//std::cout << points_batch->colors[ii][0] << " " << points_batch->colors[ii][1] << std::endl;
    			// write the point
    			laswriter->write_point(&laspoint);
    			// add it to the inventory
    			laswriter->update_inventory(&laspoint);
		}
	    }
	    //process point processors
            points_batch->trajectory_id = trajectory_id;
            pipeline.back()->Process(std::move(points_batch));
          }
          delayed_messages.pop_front();
        }
        delayed_messages.push_back(message);
        LOG_EVERY_N(INFO, 100000)
            << "Processed " << (message.getTime() - begin_time).toSec()
            << " of " << duration_in_seconds << " bag time seconds...";
      }
      bag.close();
      
      if(usingLas){
      	// update the header
      	laswriter->update_header(&lasheader, TRUE);
      	// close the writer
      	I64 total_bytes = laswriter->close();
      }
    }
  } while (pipeline.back()->Flush() ==
           carto::io::PointsProcessor::FlushResult::kRestartStream);
}

}  // namespace
}  // namespace cartographer_ros

int main(int argc, char** argv) {
  FLAGS_alsologtostderr = true;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  CHECK(!FLAGS_configuration_directory.empty())
      << "-configuration_directory is missing.";
  CHECK(!FLAGS_configuration_basename.empty())
      << "-configuration_basename is missing.";
  CHECK(!FLAGS_bag_filenames.empty()) << "-bag_filenames is missing.";
  CHECK(!FLAGS_pose_graph_filename.empty())
      << "-pose_graph_filename is missing.";

  //opening trajectory file
  trajectoryName = FLAGS_pose_graph_filename + "_traj.txt";
  //std::ofstream outfile;
  //outfile.open(trajectoryName, std::ios_base::in);
  //outfile.close();

  ::cartographer_ros::Run(
      FLAGS_pose_graph_filename,
      cartographer_ros::SplitString(FLAGS_bag_filenames, ','),
      FLAGS_configuration_directory, FLAGS_configuration_basename,
      FLAGS_urdf_filename, FLAGS_output_file_prefix);
}
